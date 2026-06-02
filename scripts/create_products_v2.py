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
    existing_skus: list = field(default_factory=list)         # for refresh hvis options ændres
    new_variants: list = field(default_factory=list)          # list of VariantSpec


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
    """Hent primary fulfillment location ID."""
    q = """
    query { locations(first: 5) {
      edges { node { id name isPrimary } }
    } }
    """
    d = gql(q)
    edges = d['data']['locations']['edges']
    primary = next((e['node'] for e in edges if e['node'].get('isPrimary')), edges[0]['node'])
    print(f"📍 Primary location: {primary['name']} ({primary['id']})")
    return primary['id']


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

        spec = MergeSpec(
            existing_handle=existing_handle,
            options_to_add=options_to_add,
            existing_skus=existing_skus if needs_refresh else [],
        )

        # Refresh existing variants if new options need adding
        if needs_refresh:
            for ex_sku in existing_skus:
                if ex_sku not in feed_by_sku: continue
                ex_row = feed_by_sku[ex_sku]
                ex_opts = all_variant_map.get(ex_sku, {})
                if not ex_opts: continue
                ordered = order_opts(ex_opts)
                opts_dict = dict(ordered)
                all_images_var = get_all_images(ex_row)
                raw_html_var = clean_vidaxl(ex_row.get('HTML_description', ''))
                vspec = _row_to_variant_spec(
                    ex_row, ex_sku, opts_dict, set(), pricing_cfg,
                    is_first=False, all_images=all_images_var, raw_html=raw_html_var
                )
                # Set ordered option_values explicitly
                vspec.option_values = ordered
                spec.new_variants.append(vspec)

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
      variants(first: 100) { edges { node { id sku } } }
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

    # Build files (media)
    files = [{"originalSource": url, "contentType": "IMAGE"} for url in spec.media_urls]

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


def call_variants_merge(merge: MergeSpec, location_id: str) -> dict:
    """Add new variants to existing product."""
    product_id = find_product_by_handle(merge.existing_handle)
    if not product_id:
        raise Exception(f"Product handle '{merge.existing_handle}' findes ikke i Shopify")

    # Get current options to compute ordered option list
    cur_options = fetch_product_options(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, merge.existing_handle)
    full_options = list(cur_options) + [o for o in merge.options_to_add if o not in cur_options]

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
            "sku": v.sku,
            "barcode": v.barcode if v.barcode else None,
            "inventoryItem": {
                "cost": str(v.cost),
                "tracked": True,
                "requiresShipping": True,
                "measurement": {"weight": {"value": v.weight_grams / 1000.0, "unit": "KILOGRAMS"}},
            },
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
            var_in["compareAtPrice"] = str(v.compare_at_price)
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

    # 1. Create new products
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
                print(f"  [{i}] ✅ {p['handle']}: {p['title'][:60]} ({len(p['variants']['edges'])} variants)")
        except Exception as e:
            stats["errors"] += 1
            print(f"  [{i}] ❌ {spec.handle}: {str(e)[:200]}")

    # 2. Merge variants into existing products
    print(f"\n🚀 Tilføjer variants til {len(merge_specs)} eksisterende produkter...")
    for i, merge in enumerate(merge_specs, 1):
        try:
            res = call_variants_merge(merge, location_id)
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
        print(f"\n📊 STATS: {stats['created_products']} created, "
              f"{stats['merged_products']} merged ({stats['merged_variants']} new variants), "
              f"{stats['errors']} errors")

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
