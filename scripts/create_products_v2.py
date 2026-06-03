"""Direct-API replacement for create_products.py.

Bevarer al scrape/group-logik fra v1 (samme imports fra product_utils). Forskel:
  - OLD: build_new_products() bygger Matrixify-XLSX → Matrixify importerer → Shopify
  - NEW: build_product_specs() bygger struktureret data → productSet GraphQL → Shopify
         (productSet håndterer produkt + varianter + media + metafields i ÉN kald).

Merge variants:
  - OLD: matrixify_create_merge.xlsx → Matrixify
  - NEW: productVariantsBulkCreate på eksisterende produkt-handle

Sikkerhed under test:
  --dry-run (default): bygger specs, dumper til JSON. Ingen Shopify-kald.
  --live: faktisk oprettelse i Shopify.
  --limit N: max N nye produkter (test-mode for at validere på små batches).
"""
import argparse
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing
from product_utils import (
    WARMUP_DAYS, build_tags, clean_title_from_options, clean_vidaxl,
    count_combinations, fetch_feed, fetch_product_options, fetch_shopify_data,
    fetch_variant_skus, format_body_html, generate_handle,
    generate_seo_description, get_all_images, load_config, normalize_sku,
    scrape_vidaxl, title_case_danish, fix_pcs_to_dele, validate_url,
    upsert_warmup_state,
)


# === CONFIG ============================================================
FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
MAX_GROUPS = int(os.environ.get('MAX_PRODUCTS_PER_RUN', '999'))
MAX_VARIANTS_SOFT = int(os.environ.get('MAX_VARIANTS_PER_RUN', '999'))
MAX_VARIANTS_HARD = 999
MAX_COMBOS = 100
MIN_STOCK_PRIMARY = 20
MIN_STOCK_VARIANT = 4
PRODUCT_ORDER = os.environ.get('PRODUCT_ORDER', 'newest')
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           '..', 'config', 'Kategori_Config.xlsx')
SKIPPED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            '..', 'output', 'skipped_large_products.json')
COUNT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          '..', 'output', 'daily_variant_count.txt')
SPECS_DUMP_PATH = "output/product_specs_v2.json"

GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN, 'Content-Type': 'application/json'}


# === STRUKTUREREDE DATA ================================================

@dataclass
class VariantSpec:
    sku: str
    price: int
    cost: float
    weight_grams: int
    inventory_quantity: int
    barcode: str = ''
    compare_at_price: Optional[int] = None
    option_values: list = field(default_factory=list)        # [('Color', 'Red'), ...]
    image_url: Optional[str] = None                           # variant-specifikt billede
    metafields: list = field(default_factory=list)            # [{namespace, key, type, value}]
    google_mpn: str = ''


@dataclass
class ProductSpec:
    handle: str
    title: str
    body_html: str
    vendor: str
    product_type: str
    tags: list
    status: str = 'ACTIVE'                                    # ACTIVE / DRAFT / ARCHIVED
    seo_title: str = ''
    seo_description: str = ''
    options_definition: list = field(default_factory=list)    # ['Color', 'Size']
    media_urls: list = field(default_factory=list)            # produkt-niveau billeder
    variants: list = field(default_factory=list)              # list of VariantSpec


@dataclass
class MergeSpec:
    """Bruges når nye varianter skal tilføjes til EKSISTERENDE produkt (samme handle)."""
    existing_handle: str
    options_to_add: list = field(default_factory=list)        # nye option-navne der ikke fandtes
    existing_skus: list = field(default_factory=list)         # eksisterende SKUs paa produktet
    new_variants: list = field(default_factory=list)          # list of VariantSpec
    # NOEDVENDIG for productSet-baseret merge naar options_to_add ikke er tom:
    # map fra existing_sku -> ordnet liste af (option_name, option_value) tupler fra scrape.
    # Bruges til at tildele KORREKTE option-vaerdier til eksisterende varianter
    # naar nye options tilfoejes til produktet.
    existing_variant_options: dict = field(default_factory=dict)


# === GRAPHQL HJÆLPERE ==================================================

def gql(query, variables=None):
    payload = {'query': query}
    if variables: payload['variables'] = variables
    for attempt in range(1, 5):
        r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=120)
        r.raise_for_status()
        d = r.json()
        if 'errors' in d:
            throttled = any('Throttled' in str(e) or 'THROTTLED' in str(e) for e in d['errors'])
            if throttled and attempt < 4:
                time.sleep(2 ** attempt); continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        cost = d.get('extensions', {}).get('cost', {}).get('throttleStatus', {})
        if cost.get('currentlyAvailable', 1000) < 200:
            time.sleep(0.5)
        return d
    raise Exception("Max retries exceeded")


def get_primary_location_id():
    """Returner hardcoded primary location GID.

    Dropxl-tokenen mangler read_locations scope (var ikke noedvendigt under
    Matrixify). Vi hardcoder ID'et hentet fra vidaxl-pris-lager's
    update_shop_cache.py outputs (verificeret 2026-06-02).

    Senere: tilfoej read_locations til access scope i Shopify Custom App,
    eller saet LOCATION_ID som GitHub var.
    """
    loc_id = os.environ.get('LOCATION_ID', '97768178013')
    print(f"📍 Primary location: gid://shopify/Location/{loc_id}")
    return f"gid://shopify/Location/{loc_id}"


def find_product_by_handle(handle: str) -> Optional[str]:
    """Returnér product_id (gid) eller None."""
    q = """
    query($handle: String!) { productByHandle(handle: $handle) { id } }
    """
    d = gql(q, {'handle': handle})
    p = d['data'].get('productByHandle')
    return p['id'] if p else None


# === SPEC-BUILDERS (parallel til build_new_products / build_merge_variants) ==

def _row_to_variant_spec(row, sku: str, variant_map_opts: dict, irrelevant: set,
                         pricing_cfg, is_first: bool, all_images: list,
                         raw_html: str) -> VariantSpec:
    """Common variant-spec builder. is_first: kun den første variant har 'master' billede."""
    cost_kr = float(row['B2B price'])
    price = pricing.calculate_normal_price(cost_kr, pricing_cfg)

    weight = 0
    if pd.notna(row.get('Weight')):
        try: weight = int(float(str(row['Weight']).replace(',', '.')) * 1000)
        except: pass

    # Variant options (filtreret for irrelevante)
    relevant = {k: v for k, v in variant_map_opts.items() if k not in irrelevant}
    opt_list = list(relevant.items())

    # Variant metafields — kun for ikke-første variant (første har data på produkt-niveau)
    mfields = [
        {"namespace": "custom", "key": "sku", "type": "single_line_text_field", "value": sku},
    ]
    if not is_first:
        if raw_html:
            mfields.append({"namespace": "custom", "key": "produktinfo",
                           "type": "multi_line_text_field", "value": raw_html})
        if all_images:
            mfields.append({"namespace": "custom", "key": "variantbilleder",
                           "type": "list.single_line_text_field",
                           "value": json.dumps(all_images)})

    return VariantSpec(
        sku=sku,
        price=int(price),
        cost=cost_kr,
        weight_grams=weight,
        inventory_quantity=int(row.get('Stock', 0) or 0),
        barcode=str(row.get('EAN', '')) if pd.notna(row.get('EAN')) else '',
        compare_at_price=None,    # warmup → ingen compareAt
        option_values=opt_list,
        image_url=all_images[0] if all_images else None,
        metafields=mfields,
        google_mpn=sku,
    )


def build_product_specs(product_groups, config, underkat, rum_dict,
                        existing_handles, feed, pricing_cfg=None) -> list:
    """Bygger ProductSpec[] for completely new products (Command=MERGE i Matrixify
    var faktisk 'create or merge by handle' — vi her 100% creates fordi handle er nyt)."""
    specs = []
    handles_used = existing_handles.copy()

    for group in product_groups:
        if group.get('is_merge', False): continue

        feed_rows = group['feed_rows']
        if isinstance(feed_rows, list):
            feed_rows = feed[feed['SKU'].isin(feed_rows)]
        if len(feed_rows) == 0: continue

        variant_map = group['variant_map']
        option_struct = group.get('options', {})
        first = feed_rows.iloc[0]

        # === Titel ===
        all_opt_displays = set()
        for od in option_struct.values():
            for v in od.get('values', []): all_opt_displays.add(v['display'])
        for _, fr in feed_rows.iterrows():
            if pd.notna(fr.get('Color')): all_opt_displays.add(str(fr['Color']).strip())

        raw_title = str(first['Title']) if pd.notna(first['Title']) else ''
        sorted_displays = sorted(list(all_opt_displays), key=len, reverse=True)
        clean_t = clean_title_from_options(raw_title, sorted_displays)
        final_title = title_case_danish(clean_t)
        if not final_title or len(final_title) < 5:
            final_title = title_case_danish(fix_pcs_to_dele(clean_vidaxl(raw_title)))

        handle = generate_handle(final_title, handles_used)

        # === Irrelevante options (kun 1 unik værdi) ===
        if len(variant_map) > 1:
            all_ov = defaultdict(set)
            for opts in variant_map.values():
                for k, v in opts.items(): all_ov[k].add(v)
            irrelevant = {k for k, v in all_ov.items() if len(v) <= 1}
        else:
            irrelevant = set()

        # === Options-definition (rækkefølge fra første variant's options) ===
        options_def = []
        for opts in variant_map.values():
            for k in opts:
                if k not in irrelevant and k not in options_def:
                    options_def.append(k)

        # === Product-level fields fra første row ===
        body_html = format_body_html(first.get('HTML_description', ''))
        product_type = first['Category'].split(' > ')[-1].strip() if pd.notna(first['Category']) else ''
        seo_title = final_title[:70] if len(final_title) <= 70 else final_title[:67] + '...'
        seo_desc = generate_seo_description(body_html)
        tags = build_tags(first, rum_dict)
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(',') if t.strip()]

        # Master images = fra første variant. Resten har egne billeder via variant metafields.
        first_all_images = get_all_images(first)

        spec = ProductSpec(
            handle=handle,
            title=final_title,
            body_html=body_html,
            vendor=str(first.get('Brand', '') or 'vidaXL'),
            product_type=product_type,
            tags=tags,
            status='ACTIVE',
            seo_title=seo_title,
            seo_description=seo_desc,
            options_definition=options_def,
            media_urls=first_all_images,    # produkt-niveau master-billeder
            variants=[],
        )

        # === Variants ===
        is_first = True
        for _, row in feed_rows.iterrows():
            try:
                sku = normalize_sku(row['SKU'])
                opts = variant_map.get(sku, {})
                all_images_var = get_all_images(row)
                raw_html_var = clean_vidaxl(row.get('HTML_description', ''))

                vspec = _row_to_variant_spec(
                    row, sku, opts, irrelevant, pricing_cfg,
                    is_first=is_first, all_images=all_images_var,
                    raw_html=raw_html_var
                )
                spec.variants.append(vspec)
                is_first = False
            except Exception as e:
                print(f"   ⚠ Variant fejl SKU {row.get('SKU','?')}: {str(e)[:100]}")

        if spec.variants:
            specs.append(spec)
            print(f"   ✅ Spec: {spec.title[:60]} ({len(spec.variants)} variants, options={options_def})")

    return specs


def build_merge_specs(product_groups, config, underkat, store, token, feed, pricing_cfg=None) -> list:
    """Bygger MergeSpec[] for nye varianter på eksisterende produkter."""
    specs = []
    feed_by_sku = {}
    for _, r in feed.iterrows():
        s = normalize_sku(r['SKU'])
        if s and s not in feed_by_sku: feed_by_sku[s] = r

    for group in product_groups:
        if not group.get('is_merge', False): continue

        feed_rows = group['feed_rows']
        if isinstance(feed_rows, list):
            feed_rows = feed[feed['SKU'].isin(feed_rows)]
        if len(feed_rows) == 0: continue

        variant_map = group['variant_map']
        existing_handle = group['existing_handle']
        existing_skus = group.get('existing_skus', [])
        all_variant_map = group.get('all_variant_map', {})

        existing_option_names = fetch_product_options(store, token, existing_handle)
        new_option_names = set()
        for opts in variant_map.values():
            new_option_names.update(opts.keys())
        options_to_add = list(new_option_names - set(existing_option_names)) if existing_option_names else []
        needs_refresh = bool(options_to_add)

        def order_opts(opts):
            ordered = []
            if existing_option_names:
                for opt_name in existing_option_names:
                    if opt_name in opts: ordered.append((opt_name, opts[opt_name]))
                for k, v in opts.items():
                    if k not in existing_option_names: ordered.append((k, v))
            else:
                ordered = list(opts.items())
            return ordered

        # Hvis nye options skal tilfoejes: gem scrape-derived option-vaerdier for
        # eksisterende SKUs saa vi kan kalde productSet med korrekte vaerdier.
        existing_variant_options = {}
        if options_to_add:
            for ex_sku in existing_skus:
                ex_opts = all_variant_map.get(ex_sku, {})
                if ex_opts:
                    existing_variant_options[ex_sku] = order_opts(ex_opts)

        spec = MergeSpec(
            existing_handle=existing_handle,
            options_to_add=options_to_add,
            existing_skus=existing_skus,
            existing_variant_options=existing_variant_options,
        )

        # Add new variants
        for _, row in feed_rows.iterrows():
            try:
                sku = normalize_sku(row['SKU'])
                opts = variant_map.get(sku, {})
                ordered = order_opts(opts)
                opts_dict = dict(ordered)
                all_images_var = get_all_images(row)
                raw_html_var = clean_vidaxl(row.get('HTML_description', ''))
                vspec = _row_to_variant_spec(
                    row, sku, opts_dict, set(), pricing_cfg,
                    is_first=False, all_images=all_images_var, raw_html=raw_html_var
                )
                vspec.option_values = ordered
                spec.new_variants.append(vspec)
            except Exception as e:
                print(f"   ⚠ Merge fejl SKU {row.get('SKU','?')}: {str(e)[:100]}")

        if spec.new_variants:
            specs.append(spec)
            print(f"   ✅ MergeSpec: {existing_handle} ({len(spec.new_variants)} variants, options_to_add={options_to_add})")

    return specs


# === GRAPHQL MUTATIONS ================================================

PRODUCT_SET_MUTATION = """
mutation productSet($input: ProductSetInput!, $synchronous: Boolean) {
  productSet(input: $input, synchronous: $synchronous) {
    product {
      id title handle
      variants(first: 250) { edges { node { id sku } } }
    }
    productSetOperation { id status }
    userErrors { field message code }
  }
}
"""

VARIANTS_BULK_CREATE = """
mutation productVariantsBulkCreate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkCreate(productId: $productId, variants: $variants) {
    productVariants { id sku }
    userErrors { field message code }
  }
}
"""

# Upload nye media-files til produktet — bruges foer productVariantsBulkCreate
# saa varianter kan referere til mediaId (mediaSrc i bulkCreate linker ikke
# media til variant — det opretter kun media paa produktet).
PRODUCT_CREATE_MEDIA = """
mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
  productCreateMedia(productId: $productId, media: $media) {
    media { id alt status mediaContentType }
    mediaUserErrors { field message code }
  }
}
"""

# Hent alle sales channel publication IDs (caches efter foerste opslag)
PUBLICATIONS_QUERY = """
query { publications(first: 20) { edges { node { id name } } } }
"""

PUBLISH_MUTATION = """
mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    publishable { ... on Product { id } }
    userErrors { field message }
  }
}
"""

# Tilfoej nye options til eksisterende produkt (merge med options_to_add)
PRODUCT_OPTIONS_CREATE = """
mutation productOptionsCreate($productId: ID!, $options: [OptionCreateInput!]!) {
  productOptionsCreate(productId: $productId, options: $options) {
    product { id options { id name values } }
    userErrors { field message code }
  }
}
"""

# Hent fuld state af eksisterende produkt for productSet-baseret merge
FETCH_EXISTING_PRODUCT_STATE = """
query($id: ID!) {
  product(id: $id) {
    id
    options { id name values }
    variants(first: 250) {
      edges {
        node {
          id sku price compareAtPrice barcode taxable inventoryPolicy
          selectedOptions { name value }
          image { id }
          inventoryItem {
            sku tracked requiresShipping
            unitCost { amount }
            measurement { weight { value unit } }
          }
        }
      }
    }
  }
}
"""

_PUBLICATIONS_CACHE = None


def get_all_publications() -> list:
    """Returnerer alle sales channel publication IDs (caches per process)."""
    global _PUBLICATIONS_CACHE
    if _PUBLICATIONS_CACHE is None:
        d = gql(PUBLICATIONS_QUERY)
        edges = d.get('data', {}).get('publications', {}).get('edges', [])
        _PUBLICATIONS_CACHE = [e['node'] for e in edges]
        print(f"📡 Sales channels: {len(_PUBLICATIONS_CACHE)} ({[p['name'] for p in _PUBLICATIONS_CACHE]})")
    return _PUBLICATIONS_CACHE


def publish_to_all_channels(product_id: str) -> list:
    """Publish nyt produkt til alle sales channels (matcher v1's 'Published Scope: global')."""
    pubs = get_all_publications()
    if not pubs:
        return []
    inputs = [{"publicationId": p['id']} for p in pubs]
    d = gql(PUBLISH_MUTATION, {"id": product_id, "input": inputs})
    return d.get('data', {}).get('publishablePublish', {}).get('userErrors') or []


def _variant_to_set_input(v: VariantSpec, location_id: str, options_def: list) -> dict:
    """Convert VariantSpec → ProductVariantSetInput dict."""
    # Option values referencer option-navne fra produkt
    option_values = []
    # v.option_values er liste af (name, value) tuples — map til ProductSet's format
    opts_dict = dict(v.option_values)
    for opt_name in options_def:
        if opt_name in opts_dict:
            option_values.append({"optionName": opt_name, "name": opts_dict[opt_name]})

    inv_item = {
        "cost": str(v.cost),
        "tracked": True,
        "requiresShipping": True,
        "measurement": {"weight": {"value": v.weight_grams / 1000.0, "unit": "KILOGRAMS"}},
    }

    variant_input = {
        "optionValues": option_values if option_values else [{"optionName": "Title", "name": "Default Title"}],
        "price": str(v.price),
        "sku": v.sku,
        "barcode": v.barcode if v.barcode else None,
        "inventoryItem": inv_item,
        "inventoryPolicy": "DENY",
        "inventoryQuantities": [{
            "locationId": location_id,
            "name": "available",
            "quantity": v.inventory_quantity,
        }],
        "metafields": v.metafields,
        "taxable": True,
    }
    if v.compare_at_price is not None:
        variant_input["compareAtPrice"] = str(v.compare_at_price)
    # Variant-image via file (uploades og linkes til varianten af Shopify)
    if v.image_url:
        variant_input["file"] = {
            "originalSource": v.image_url,
            "contentType": "IMAGE",
            "alt": f"{v.sku} variant",
        }
    return variant_input


def call_product_set(spec: ProductSpec, location_id: str) -> dict:
    """Opret produkt via productSet (synkront)."""
    # Build productOptions
    if spec.options_definition:
        # Collect unique values per option from variants
        option_values_map = defaultdict(set)
        for v in spec.variants:
            for opt_name, opt_val in v.option_values:
                option_values_map[opt_name].add(opt_val)
        product_options = [
            {"name": opt, "values": [{"name": val} for val in sorted(option_values_map[opt])]}
            for opt in spec.options_definition
        ]
    else:
        # Single-variant product → default Title option
        product_options = [{"name": "Title", "values": [{"name": "Default Title"}]}]

    # Build files (media) som UNION af product-master + variant-billeder
    # Shopify kraever at variant.file.originalSource ogsaa staar i product files-arrayet.
    files = []
    seen_urls = set()
    for i, url in enumerate(spec.media_urls):
        if url in seen_urls:
            continue
        seen_urls.add(url)
        alt = f"{spec.title} - Hovedbillede" if i == 0 else f"{spec.title} - Billede {i+1}"
        files.append({"originalSource": url, "contentType": "IMAGE", "alt": alt})
    # Tilfoej variant-specifikke billeder der ikke allerede er i master-listen
    for v in spec.variants:
        if v.image_url and v.image_url not in seen_urls:
            seen_urls.add(v.image_url)
            files.append({
                "originalSource": v.image_url,
                "contentType": "IMAGE",
                "alt": f"{spec.title} - Variant {v.sku}",
            })

    # Build variants
    variants = [_variant_to_set_input(v, location_id, spec.options_definition) for v in spec.variants]

    input_payload = {
        "title": spec.title,
        "descriptionHtml": spec.body_html,
        "vendor": spec.vendor,
        "productType": spec.product_type,
        "tags": spec.tags,
        "handle": spec.handle,
        "status": spec.status,
        "seo": {"title": spec.seo_title, "description": spec.seo_description} if spec.seo_title else None,
        "productOptions": product_options,
        "variants": variants,
        "files": files if files else None,
    }
    # Remove None values
    input_payload = {k: v for k, v in input_payload.items() if v is not None}

    d = gql(PRODUCT_SET_MUTATION, {"input": input_payload, "synchronous": True})
    return d['data']['productSet']


def _call_merge_via_productset(merge: MergeSpec, product_id: str,
                                full_options: list, location_id: str) -> dict:
    """Merge med options_to_add via productSet (atomic).

    Bruges naar de nye varianter har options som det eksisterende produkt ikke
    har endnu. productOptionsCreate + productVariantsBulkCreate er IKKE atomic
    og efterlader produktet i half-state ved fejl. productSet haandterer det
    hele i een atomic mutation.

    Strategi:
      1. Fetch eksisterende variants (id, sku, selectedOptions, price etc.)
      2. Upload nye variant-billeder via productCreateMedia
      3. Byg fuld variants-liste = eksisterende (med id + nye option-vaerdier
         fra scrape) + nye varianter
      4. Kald productSet med full state

    Eksisterende varianter faar deres KORREKTE option-vaerdier (fra scrape's
    all_variant_map), ikke Shopify's auto-default.
    """
    # 1. Fetch full existing state
    d_existing = gql(FETCH_EXISTING_PRODUCT_STATE, {"id": product_id})
    p = d_existing.get('data', {}).get('product')
    if not p:
        raise Exception(f"Kunne ikke hente eksisterende produkt {product_id}")
    existing_variants = [e['node'] for e in p['variants']['edges']]
    print(f"    🔍 productSet-merge: {len(existing_variants)} eksisterende variants, +{len(merge.new_variants)} nye, options={full_options}")

    # 2. Upload nye variant-billeder (eksisterende variants har allerede deres
    # billeder i produktets media — vi rør dem ikke).
    unique_new_urls = []
    seen = set()
    for v in merge.new_variants:
        if v.image_url and v.image_url not in seen:
            seen.add(v.image_url)
            unique_new_urls.append(v.image_url)

    url_to_media_id = {}
    if unique_new_urls:
        media_input = [
            {"originalSource": url, "mediaContentType": "IMAGE",
             "alt": f"Variant billede ({i+1}/{len(unique_new_urls)})"}
            for i, url in enumerate(unique_new_urls)
        ]
        d_media = gql(PRODUCT_CREATE_MEDIA, {"productId": product_id, "media": media_input})
        media_res = d_media.get('data', {}).get('productCreateMedia', {})
        media_errs = media_res.get('mediaUserErrors') or []
        if media_errs:
            print(f"    ⚠ media-upload errors: {media_errs[:2]}")
        for url, m in zip(unique_new_urls, media_res.get('media') or []):
            url_to_media_id[url] = m['id']
        print(f"    📷 Uploadede {len(url_to_media_id)} variant-billeder til produktet")

    # 3. Saml alle unikke option-vaerdier per option (eksisterende + nye)
    option_values_map = defaultdict(set)
    # Fra eksisterende variants (deres nuvaerende selectedOptions)
    for ev in existing_variants:
        for so in ev['selectedOptions']:
            if so['name'] in full_options:
                option_values_map[so['name']].add(so['value'])
    # Fra scrape-data for eksisterende SKUs (de KORREKTE nye option-vaerdier)
    for sku, opts in merge.existing_variant_options.items():
        for ov_name, ov_val in opts:
            if ov_name in full_options:
                option_values_map[ov_name].add(ov_val)
    # Fra nye varianter
    for v in merge.new_variants:
        for ov_name, ov_val in v.option_values:
            if ov_name in full_options:
                option_values_map[ov_name].add(ov_val)

    product_options_input = [
        {"name": opt, "values": [{"name": val} for val in sorted(option_values_map[opt])]}
        for opt in full_options
    ]

    # 4. Byg variants-liste

    def _option_values_for_existing(ev) -> list:
        """Byg optionValues for en eksisterende variant.

        Bruger scrape-data hvis tilgaengeligt for nye options, ellers fallback
        til Shopify's nuvaerende selectedOptions eller foerste option-vaerdi.
        """
        sku = ev.get('sku', '')
        scrape_opts = dict(merge.existing_variant_options.get(sku, []))
        current_opts = {so['name']: so['value'] for so in ev['selectedOptions']}

        out = []
        for opt_name in full_options:
            # Prioritet: scrape > nuvaerende > foerste vaerdi
            if opt_name in scrape_opts:
                val = scrape_opts[opt_name]
            elif opt_name in current_opts:
                val = current_opts[opt_name]
            elif option_values_map[opt_name]:
                val = sorted(option_values_map[opt_name])[0]
            else:
                continue
            out.append({"optionName": opt_name, "name": val})
        return out

    variants_input = []

    # Eksisterende variants — PATCH-style: id + optionValues, intet andet (bevarer pris/cost/lager).
    for ev in existing_variants:
        variants_input.append({
            "id": ev['id'],
            "optionValues": _option_values_for_existing(ev),
        })

    # Nye varianter — fuld state
    for v in merge.new_variants:
        opt_vals = []
        opts_dict = dict(v.option_values)
        for opt_name in full_options:
            if opt_name in opts_dict:
                opt_vals.append({"optionName": opt_name, "name": opts_dict[opt_name]})
            elif option_values_map[opt_name]:
                # Variant har ikke det option — brug foerste vaerdi som default
                opt_vals.append({"optionName": opt_name, "name": sorted(option_values_map[opt_name])[0]})

        inv_item = {
            "sku": v.sku,
            "cost": str(v.cost),
            "tracked": True,
            "requiresShipping": True,
            "measurement": {"weight": {"value": v.weight_grams / 1000.0, "unit": "KILOGRAMS"}},
        }
        var_in = {
            "optionValues": opt_vals,
            "price": str(v.price),
            "sku": v.sku,
            "barcode": v.barcode if v.barcode else None,
            "taxable": True,
            "inventoryPolicy": "DENY",
            "inventoryItem": inv_item,
            "inventoryQuantities": [{
                "locationId": location_id,
                "name": "available",
                "quantity": v.inventory_quantity,
            }],
            "metafields": v.metafields,
        }
        if v.compare_at_price is not None:
            var_in["compareAtPrice"] = str(v.compare_at_price)
        if v.image_url and v.image_url in url_to_media_id:
            var_in["file"] = {"id": url_to_media_id[v.image_url]}
        var_in = {k: vv for k, vv in var_in.items() if vv is not None}
        variants_input.append(var_in)

    # 5. Kald productSet
    input_payload = {
        "id": product_id,
        "productOptions": product_options_input,
        "variants": variants_input,
    }
    d = gql(PRODUCT_SET_MUTATION, {"input": input_payload, "synchronous": True})
    res = d['data']['productSet']
    # Normalisér output til samme format som productVariantsBulkCreate's response
    # saa apply_specs kan haandtere det uniformt.
    user_errs = res.get('userErrors') or []
    if user_errs:
        return {"productVariants": [], "userErrors": user_errs}
    # Tael KUN de nye varianter (ikke de eksisterende vi opdaterede)
    all_variants = (res.get('product') or {}).get('variants', {}).get('edges', [])
    new_skus_set = {v.sku for v in merge.new_variants}
    new_variants_created = [e['node'] for e in all_variants
                            if e['node']['sku'] in new_skus_set]
    return {"productVariants": new_variants_created, "userErrors": []}


def call_variants_merge(merge: MergeSpec, location_id: str) -> dict:
    """Add new variants to existing product.

    NB: productVariantsBulkInput har ANDEN struktur end ProductVariantSetInput:
      - sku er IKKE top-level — den ligger inde i inventoryItem.sku
      - inventoryQuantities bruger availableQuantity (ikke name+quantity)
      - measurement.weight bruger value+unit struktur (samme)

    Hvis merge.options_to_add ikke er tom: kalder vi productOptionsCreate FOERST
    saa Shopify accepterer de nye option-vaerdier paa varianterne.
    """
    product_id = find_product_by_handle(merge.existing_handle)
    if not product_id:
        raise Exception(f"Product handle '{merge.existing_handle}' findes ikke i Shopify")

    cur_options = fetch_product_options(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, merge.existing_handle)
    full_options = list(cur_options) + [o for o in merge.options_to_add if o not in cur_options]

    # Hvis nye options skal tilfoejes: brug productSet-baseret merge.
    # productSet er ATOMIC og haandterer option-tilfoejelse + variant-opdatering +
    # variant-creation i een enkelt mutation. Det undgaar half-mutation-bugget
    # vi havde med productOptionsCreate + productVariantsBulkCreate.
    if merge.options_to_add:
        return _call_merge_via_productset(merge, product_id, full_options, location_id)

    # Upload alle unikke variant-billeder til produktet FOERST.
    # Vi bruger productCreateMedia og gemmer URL -> mediaId for at kunne
    # referere via mediaId i productVariantsBulkCreate (mediaSrc-feltet
    # tilfoejer kun media til produktet uden at linke det til varianten).
    unique_image_urls = []
    seen = set()
    for v in merge.new_variants:
        if v.image_url and v.image_url not in seen:
            seen.add(v.image_url)
            unique_image_urls.append(v.image_url)

    url_to_media_id = {}
    if unique_image_urls:
        media_input = [
            {"originalSource": url, "mediaContentType": "IMAGE",
             "alt": f"Variant billede ({i+1}/{len(unique_image_urls)})"}
            for i, url in enumerate(unique_image_urls)
        ]
        d_media = gql(PRODUCT_CREATE_MEDIA, {"productId": product_id, "media": media_input})
        media_res = d_media.get('data', {}).get('productCreateMedia', {})
        media_errs = media_res.get('mediaUserErrors') or []
        if media_errs:
            print(f"    ⚠ media-upload errors: {media_errs[:2]}")
        # Rækkefølgen i response matcher input-rækkefølgen
        for url, m in zip(unique_image_urls, media_res.get('media') or []):
            url_to_media_id[url] = m['id']
        print(f"    📷 Uploadede {len(url_to_media_id)} variant-billeder til produktet")

    variants_input = []
    for v in merge.new_variants:
        option_values = []
        opts_dict = dict(v.option_values)
        for opt_name in full_options:
            if opt_name in opts_dict:
                option_values.append({"optionName": opt_name, "name": opts_dict[opt_name]})

        var_in = {
            "optionValues": option_values,
            "price": str(v.price),
            "barcode": v.barcode if v.barcode else None,
            "inventoryItem": {
                "sku": v.sku,                                     # SKU her, ikke top-level
                "cost": str(v.cost),
                "tracked": True,
                "requiresShipping": True,
                "measurement": {"weight": {"value": v.weight_grams / 1000.0, "unit": "KILOGRAMS"}},
            },
            "inventoryPolicy": "DENY",
            "inventoryQuantities": [{
                "locationId": location_id,
                "availableQuantity": v.inventory_quantity,        # availableQuantity, ikke name+quantity
            }],
            "metafields": v.metafields,
            "taxable": True,
        }
        if v.compare_at_price is not None:
            var_in["compareAtPrice"] = str(v.compare_at_price)
        # Variant-image via mediaId (refererer til just-uploaded media paa produktet)
        if v.image_url and v.image_url in url_to_media_id:
            var_in["mediaId"] = url_to_media_id[v.image_url]
        var_in = {k: vv for k, vv in var_in.items() if vv is not None}
        variants_input.append(var_in)

    d = gql(VARIANTS_BULK_CREATE, {"productId": product_id, "variants": variants_input})
    return d['data']['productVariantsBulkCreate']


# === APPLY ============================================================

def apply_specs(product_specs: list, merge_specs: list, location_id: str, limit: Optional[int] = None):
    """Push specs til Shopify. Logger result. Returner stats."""
    if limit is not None:
        print(f"⚠ LIMIT={limit}: kun de første {limit} produkter oprettes")
        product_specs = product_specs[:limit]
        merge_specs = merge_specs[:limit]

    stats = {"created_products": 0, "merged_products": 0, "merged_variants": 0,
             "errors": 0, "products": [], "merges": []}

    # 1. Create new products + publish til alle sales channels
    print(f"\n🚀 Opretter {len(product_specs)} nye produkter via productSet...")
    for i, spec in enumerate(product_specs, 1):
        try:
            res = call_product_set(spec, location_id)
            errs = res.get('userErrors') or []
            if errs:
                stats["errors"] += 1
                print(f"  [{i}] ❌ {spec.handle}: {errs[:2]}")
            else:
                p = res['product']
                stats["created_products"] += 1
                stats["products"].append({
                    "handle": p['handle'],
                    "id": p['id'],
                    "title": p['title'],
                    "variant_count": len(p['variants']['edges']),
                })
                # Publish til alle sales channels (svarer til v1's 'Published Scope: global')
                pub_errs = publish_to_all_channels(p['id'])
                pub_status = "✅ published" if not pub_errs else f"⚠ publish errors: {pub_errs[:1]}"
                print(f"  [{i}] ✅ {p['handle']}: {p['title'][:60]} ({len(p['variants']['edges'])} variants) — {pub_status}")
        except Exception as e:
            stats["errors"] += 1
            print(f"  [{i}] ❌ {spec.handle}: {str(e)[:200]}")

    # 2. Merge variants into existing products
    print(f"\n🚀 Tilføjer variants til {len(merge_specs)} eksisterende produkter...")
    stats["skipped_merges"] = 0
    for i, merge in enumerate(merge_specs, 1):
        try:
            res = call_variants_merge(merge, location_id)
            if res.get('_skipped'):
                stats["skipped_merges"] += 1
                continue  # printet allerede i call_variants_merge
            errs = res.get('userErrors') or []
            if errs:
                stats["errors"] += 1
                print(f"  [{i}] ❌ {merge.existing_handle}: {errs[:2]}")
            else:
                created = res.get('productVariants') or []
                stats["merged_products"] += 1
                stats["merged_variants"] += len(created)
                stats["merges"].append({
                    "handle": merge.existing_handle,
                    "new_variant_count": len(created),
                })
                print(f"  [{i}] ✅ {merge.existing_handle}: +{len(created)} variants")
        except Exception as e:
            stats["errors"] += 1
            print(f"  [{i}] ❌ {merge.existing_handle}: {str(e)[:200]}")

    return stats


# === MAIN =============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true',
                        help="Faktisk oprettelse i Shopify. Default er dry-run (skriver JSON).")
    parser.add_argument('--limit', type=int, default=None,
                        help="Max produkter (sikkerhed under test). Gælder for både new + merge.")
    args = parser.parse_args()

    DRY_RUN = not args.live

    missing = [n for n, v in [('FEED_URL', FEED_URL), ('SHOPIFY_STORE', SHOPIFY_STORE),
                              ('SHOPIFY_ACCESS_TOKEN', SHOPIFY_ACCESS_TOKEN)] if not v]
    if missing: sys.exit(f"❌ Manglende env: {', '.join(missing)}")

    mode = "LIVE" if args.live else "DRY-RUN"
    limit_str = f" [limit={args.limit}]" if args.limit else ""
    print(f"🚀 create_products_v2 — {mode}{limit_str}")

    # ===== Replikér v1's scrape/group pipeline =====
    feed = fetch_feed(FEED_URL)
    feed['SKU'] = feed['SKU'].apply(normalize_sku)
    feed['Stock'] = pd.to_numeric(feed['Stock'], errors='coerce').fillna(0)
    feed['B2B price'] = pd.to_numeric(feed['B2B price'], errors='coerce').fillna(0)
    print(f"✅ {len(feed):,} produkter i feed")

    sku_to_handle, all_handles = fetch_shopify_data(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN)
    shopify_skus = set(sku_to_handle.keys())

    feed_by_sku = {}
    for _, r in feed.iterrows():
        s = normalize_sku(r['SKU'])
        if s and s not in feed_by_sku: feed_by_sku[s] = r

    config, underkat, rum_dict, _ = load_config(CONFIG_PATH)
    aktive = config[config['Import?'] == 'JA']['Kategori_Config'].tolist()
    pricing_cfg = pricing.load_pricing_config()

    candidates = feed[
        (~feed['SKU'].isin(shopify_skus)) &
        (feed['Stock'] >= MIN_STOCK_PRIMARY) &
        (feed['B2B price'] > 0)
    ].copy()
    candidates['Hovedkategori'] = candidates['Category'].str.split(' > ').str[0]
    candidates = candidates[candidates['Hovedkategori'].isin(aktive)].copy()

    if PRODUCT_ORDER == 'random':
        candidates = candidates.sample(frac=1, random_state=int(time.time()) % 10000).reset_index(drop=True)
    else:
        candidates['SKU_num'] = pd.to_numeric(candidates['SKU'], errors='coerce')
        candidates = candidates.sort_values('SKU_num', ascending=False).reset_index(drop=True)

    print(f"✅ {len(candidates):,} kandidater")

    # Load skipped
    skipped = {}
    if os.path.exists(SKIPPED_PATH):
        try:
            with open(SKIPPED_PATH, 'r', encoding='utf-8') as f:
                skipped = json.load(f)
            skipped = {k: v for k, v in skipped.items() if v.get('status') != 'done'}
        except: skipped = {}

    # Scrape og grupper
    print(f"\n🔍 Scraper og grupperer (max {MAX_COMBOS} kombinationer)...")
    product_groups = []
    processed_skus = set()
    total_variants = 0
    skipped_count = 0

    for _, row in candidates.iterrows():
        sku = normalize_sku(row['SKU'])
        if sku in processed_skus: continue
        if len(product_groups) >= MAX_GROUPS: break
        if total_variants >= MAX_VARIANTS_SOFT: break

        url = row.get('Link', '')
        if not validate_url(url):
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}}, 'options': {},
                'existing_handle': None, 'is_merge': False,
            })
            total_variants += 1
            continue

        print(f"\n📦 [{len(product_groups)+1}] SKU {sku}...")
        scrape = scrape_vidaxl(url)
        time.sleep(1)

        if not scrape['success'] or not scrape['master_pid'] or not scrape['options']:
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}}, 'options': {},
                'existing_handle': None, 'is_merge': False,
            })
            total_variants += 1
            print(f"   → Single produkt")
            continue

        num_combos = count_combinations(scrape['options'])
        if num_combos >= MAX_COMBOS:
            skipped_count += 1
            pid = scrape['master_pid']
            if pid not in skipped:
                skipped[pid] = {
                    'status': 'pending', 'handle': None, 'url': url,
                    'title': str(row.get('Title', '')),
                    'master_pid': pid, 'sku': sku, 'num_combos': num_combos,
                    'options': scrape['options'], 'variant_map': None,
                    'created_skus': [], 'remaining_skus': None,
                }
            processed_skus.add(sku)
            continue

        variant_map = fetch_variant_skus(scrape['master_pid'], scrape['options'])
        if not variant_map:
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}}, 'options': {},
                'existing_handle': None, 'is_merge': False,
            })
            total_variants += 1
            continue

        new_skus = []
        existing_skus_in_group = []
        existing_handle_for_group = None

        for v_sku in variant_map.keys():
            if v_sku in shopify_skus:
                existing_skus_in_group.append(v_sku)
                if not existing_handle_for_group:
                    existing_handle_for_group = sku_to_handle.get(v_sku)
            elif v_sku in processed_skus: continue
            elif v_sku not in feed_by_sku: continue
            else:
                fr = feed_by_sku[v_sku]
                stock = float(fr.get('Stock', 0) or 0)
                price = float(fr.get('B2B price', 0) or 0)
                if stock >= MIN_STOCK_VARIANT and price > 0:
                    new_skus.append(v_sku)

        if not new_skus:
            processed_skus.add(sku); continue
        if total_variants + len(new_skus) > MAX_VARIANTS_HARD:
            processed_skus.add(sku); continue

        is_merge = existing_handle_for_group is not None
        group_feed = feed[feed['SKU'].isin(new_skus)].copy()
        new_variant_map = {s: variant_map[s] for s in new_skus if s in variant_map}
        for s in new_skus: processed_skus.add(s)

        product_groups.append({
            'feed_rows': group_feed, 'variant_map': new_variant_map,
            'options': scrape['options'],
            'existing_handle': existing_handle_for_group,
            'is_merge': is_merge,
            'existing_skus': existing_skus_in_group if is_merge else [],
            'all_variant_map': variant_map if is_merge else {},
        })
        total_variants += len(new_skus)
        print(f"   → {'MERGE til ' + existing_handle_for_group if is_merge else 'NYT'} ({len(new_skus)} variants, total {total_variants})")

    merges = sum(1 for g in product_groups if g['is_merge'])
    news = len(product_groups) - merges
    print(f"\n✅ {len(product_groups)} grupper ({news} nye, {merges} merge), {total_variants} variants")

    # ===== Byg specs =====
    print(f"\n📝 Bygger ProductSpecs...")
    product_specs = build_product_specs(product_groups, config, underkat, rum_dict, all_handles, feed, pricing_cfg)
    merge_specs = build_merge_specs(product_groups, config, underkat, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, feed, pricing_cfg)

    # ===== Dump specs til JSON (audit/debug) =====
    os.makedirs("output", exist_ok=True)
    with open(SPECS_DUMP_PATH, 'w', encoding='utf-8') as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
            "product_specs": [asdict(s) for s in product_specs],
            "merge_specs": [asdict(s) for s in merge_specs],
        }, f, default=str, indent=2)
    print(f"💾 Specs gemt: {SPECS_DUMP_PATH}")

    # ===== Apply hvis --live =====
    if args.live:
        location_id = get_primary_location_id()
        stats = apply_specs(product_specs, merge_specs, location_id, limit=args.limit)
        skipped_merges = stats.get('skipped_merges', 0)
        skip_str = f", {skipped_merges} merge skipped (options_to_add)" if skipped_merges else ""
        print(f"\n📊 STATS: {stats['created_products']} created, "
              f"{stats['merged_products']} merged ({stats['merged_variants']} new variants), "
              f"{stats['errors']} errors{skip_str}")

        # Indsæt warmup state for nye SKUs
        state_records = []
        warmup_until = (datetime.now(timezone.utc) + timedelta(days=WARMUP_DAYS)).isoformat()
        for spec in product_specs:
            for v in spec.variants:
                state_records.append({
                    'sku': v.sku,
                    'pricing_group': pricing.assign_group(v.sku),
                    'status': 'warmup',
                    'b2b_cost': v.cost,
                    'normal_price': v.price,
                    'sale_price': pricing.calculate_sale_price(v.cost, pricing_cfg),
                    'warmup_complete_at': warmup_until,
                })
        for merge in merge_specs:
            for v in merge.new_variants:
                state_records.append({
                    'sku': v.sku,
                    'pricing_group': pricing.assign_group(v.sku),
                    'status': 'warmup',
                    'b2b_cost': v.cost,
                    'normal_price': v.price,
                    'sale_price': pricing.calculate_sale_price(v.cost, pricing_cfg),
                    'warmup_complete_at': warmup_until,
                })
        upsert_warmup_state(state_records)

        # Variant count
        with open(COUNT_PATH, 'w') as f: f.write(str(total_variants))

        # Skipped list
        with open(SKIPPED_PATH, 'w', encoding='utf-8') as f:
            json.dump(skipped, f, ensure_ascii=False, indent=2)

        if stats["errors"]: sys.exit(1)
        print(f"\n✅ SUCCESS")
    else:
        print(f"\n[DRY-RUN] specs bygget, intet pushet. {len(product_specs)} new products + {len(merge_specs)} merges.")
        print(f"For LIVE-run: tilfoej --live (evt. med --limit N for sikkerhed)")

    # GitHub Actions outputs
    gh = os.environ.get('GITHUB_OUTPUT', '')
    if gh:
        with open(gh, 'a') as f:
            f.write(f"product_count={len(product_groups)}\n")
            f.write(f"variant_count={total_variants}\n")
            f.write(f"new_count={news}\n")
            f.write(f"merge_count={merges}\n")
            f.write(f"skipped_count={len(skipped)}\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL: {e}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)
