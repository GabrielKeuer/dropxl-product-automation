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
    fetch_variant_skus, fetch_variant_options_v2, reorder_keeper_first, reorder_options_priority, format_body_html, generate_handle,
    generate_seo_description, get_all_images, load_config, normalize_sku,
    scrape_vidaxl, title_case_danish, fix_pcs_to_dele, validate_url,
    upsert_warmup_state,
)
# Kanonisk titel-motor (100%-valideret 2026-07-05) — samme logik som merge-oraklet.
from title_engine import generate_title, titlecase_feed
import title_llm  # LLM-repair-lag (graceful; slås fra med TITLE_LLM_REPAIR=0)


def _derive_create_title(first, variant_map, feed_rows):
    """Deterministisk delt titel for et nyt produkt. Returnerer (titel, akser, rå_feed_titel).
    Kun REELLE variant-akser (>1 værdi) fjernes → fast farve/størrelse/antal bevares som identitet."""
    raw_title = str(first['Title']) if pd.notna(first['Title']) else ''
    axis_vals = defaultdict(set)
    for ov in variant_map.values():
        for k, v in ov.items():
            if v: axis_vals[k].add(v)
    topts = {k: sorted(vs) for k, vs in axis_vals.items() if len(vs) > 1}
    fc = [str(fr['Color']).strip() for _, fr in feed_rows.iterrows()
          if pd.notna(fr.get('Color')) and str(fr['Color']).strip()]
    tfc = fc if len({c.lower() for c in fc}) > 1 else []
    ptype = first['Category'].split(' > ')[-1].strip() if pd.notna(first.get('Category')) else ''
    det, _ = generate_title(titlecase_feed(raw_title), topts, shared=True,
                            product_type=ptype, feed_colors=tfc, n_variants=len(feed_rows))
    if not det or len(det) < 3:
        det = title_case_danish(fix_pcs_to_dele(clean_vidaxl(raw_title)))
    return det, topts, raw_title


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
    # Regenereret titel (sat KUN når en ny akse tilføjes → den delte titel kan
    # være blevet forkert, fx enkelt-farve → farve-variant). Tom = behold titel.
    regen_title: str = ''


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
                         raw_html: str, seed=None) -> VariantSpec:
    """Common variant-spec builder. is_first: kun den første variant har 'master' billede.

    Mode-bevidst pris (pricing.resolve_variant_pricing):
      real_discount  -> (normal, None)        [tilbud sættes senere af rotation]
      fictive_discount -> (sale, førpris)      [altid på tilbud; seed = handle]
    """
    cost_kr = float(row['B2B price'])
    price, compare_at = pricing.resolve_variant_pricing(cost_kr, pricing_cfg, seed=seed, on_sale=False)

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
        compare_at_price=int(compare_at) if compare_at else None,   # fictive: førpris; real: None (rotation)
        option_values=opt_list,
        image_url=all_images[0] if all_images else None,
        metafields=mfields,
        google_mpn=sku,
    )


def build_product_specs(product_groups, config, underkat, rum_dict,
                        existing_handles, feed, pricing_cfg_resolver=None) -> list:
    """Bygger ProductSpec[] for completely new products.

    pricing_cfg_resolver: callable(vendor, product_type) -> config dict
                         Pricing Engine: per-product vendor+type config lookup.
                         Hvis None → fallback til global default (backward compat).
    """
    specs = []
    handles_used = existing_handles.copy()

    # Backward compat: hvis ingen resolver, byg en der returnerer global default
    if pricing_cfg_resolver is None:
        _default_cfg = pricing.load_pricing_config()
        pricing_cfg_resolver = lambda v, t: _default_cfg

    # === PRE-PASS: deterministiske titler + batched LLM-repair (med akse-kontekst) ===
    # LLM'en får de REELLE variant-akser → retter kun ægte fejl, ingen falske positiver.
    # Graceful: uden ANTHROPIC_API_KEY / ved fejl beholdes den deterministiske titel.
    _title_recs = []
    for _gi, _grp in enumerate(product_groups):
        if _grp.get('is_merge', False):
            continue
        _fr = _grp['feed_rows']
        if isinstance(_fr, list):
            _fr = feed[feed['SKU'].isin(_fr)]
        if len(_fr) == 0:
            continue
        _det, _axes, _raw = _derive_create_title(_fr.iloc[0], _grp['variant_map'], _fr)
        _title_recs.append({"i": _gi, "feed": titlecase_feed(_raw), "det": _det, "axes": _axes})
    _fixed = title_llm.repair_titles(_title_recs)
    _title_by_gi = {r["i"]: (_fixed.get(r["i"]) or r["det"]) for r in _title_recs}
    if _fixed:
        print(f"   🤖 LLM-repair: {len(_fixed)}/{len(_title_recs)} create-titler rettet")

    for _gi, group in enumerate(product_groups):
        if group.get('is_merge', False): continue

        feed_rows = group['feed_rows']
        if isinstance(feed_rows, list):
            feed_rows = feed[feed['SKU'].isin(feed_rows)]
        if len(feed_rows) == 0: continue

        variant_map = group['variant_map']
        option_struct = group.get('options', {})
        first = feed_rows.iloc[0]

        # Resolve pricing-config FOR DETTE PRODUKT (vendor + type) — Katalog Engine
        _vendor = str(first.get('Brand', '') or 'vidaXL')
        _ptype = first['Category'].split(' > ')[-1].strip() if pd.notna(first.get('Category')) else None
        pricing_cfg = pricing_cfg_resolver(_vendor, _ptype)
        if not pricing_cfg:
            print(f"   ⚠ Ingen pricing-config for ({_vendor}, {_ptype}) — skipper produkt")
            continue

        # === Titel: fra pre-pass (deterministisk motor + LLM-repair) ===
        final_title = _title_by_gi.get(_gi)
        if not final_title:
            final_title, _, _ = _derive_create_title(first, variant_map, feed_rows)

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
        # Iterér i SCRAPE-order (variant_map.keys()) IKKE feed-CSV-order.
        # Det bevarer vidaXL's naturlige display-rækkefølge for option-værdier
        # (1, 2, 3 / 30 cm, 60 cm, 90 cm — ikke alfabetisk 100, 30, 60).
        feed_by_sku_local = {}
        for _, row in feed_rows.iterrows():
            s = normalize_sku(row['SKU'])
            if s and s not in feed_by_sku_local:
                feed_by_sku_local[s] = row

        is_first = True
        for sku in variant_map.keys():
            if sku not in feed_by_sku_local:
                continue
            row = feed_by_sku_local[sku]
            try:
                opts = variant_map.get(sku, {})
                all_images_var = get_all_images(row)
                raw_html_var = clean_vidaxl(row.get('HTML_description', ''))

                vspec = _row_to_variant_spec(
                    row, sku, opts, irrelevant, pricing_cfg,
                    is_first=is_first, all_images=all_images_var,
                    raw_html=raw_html_var, seed=handle
                )
                spec.variants.append(vspec)
                is_first = False
            except Exception as e:
                print(f"   ⚠ Variant fejl SKU {sku}: {str(e)[:100]}")

        if spec.variants:
            specs.append(spec)
            print(f"   ✅ Spec: {spec.title[:60]} ({len(spec.variants)} variants, options={options_def})")

    return specs


def build_merge_specs(product_groups, config, underkat, store, token, feed, pricing_cfg_resolver=None) -> list:
    """Bygger MergeSpec[] for nye varianter på eksisterende produkter.

    pricing_cfg_resolver: callable(vendor, product_type) -> config (Katalog Engine).
    """
    specs = []
    feed_by_sku = {}
    for _, r in feed.iterrows():
        s = normalize_sku(r['SKU'])
        if s and s not in feed_by_sku: feed_by_sku[s] = r

    # Backward compat: hvis ingen resolver, byg en der returnerer global default
    if pricing_cfg_resolver is None:
        _default_cfg = pricing.load_pricing_config()
        pricing_cfg_resolver = lambda v, t: _default_cfg

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
        first = feed_rows.iloc[0]

        # Resolve pricing-config FOR DENNE MERGE (vendor + type af eksisterende produkt)
        _vendor = str(first.get('Brand', '') or 'vidaXL')
        _ptype = first['Category'].split(' > ')[-1].strip() if pd.notna(first.get('Category')) else None
        pricing_cfg = pricing_cfg_resolver(_vendor, _ptype)
        if not pricing_cfg:
            print(f"   ⚠ Ingen pricing-config for ({_vendor}, {_ptype}) — skipper merge")
            continue

        existing_option_names = fetch_product_options(store, token, existing_handle)
        new_option_names = set()
        for opts in variant_map.values():
            new_option_names.update(opts.keys())
        options_to_add = list(new_option_names - set(existing_option_names)) if existing_option_names else []
        # Titel-regen udløses når VARIANT-SÆTTET ændrer hvilke akser der VARIERER — ikke kun når en
        # akse-NAVN er nyt. Edge case: en akse der allerede findes med 1 værdi (fx Farve=[Hvid]) får
        # en 2. værdi → aksen er ikke i options_to_add, men titlen skal stadig strippe den nu-variant.
        def _axis_becomes_multi():
            ex_vals, new_vals = defaultdict(set), defaultdict(set)
            for s in existing_skus:
                for k, v in (all_variant_map.get(s) or {}).items():
                    if v: ex_vals[k].add(v)
            for ov in variant_map.values():
                for k, v in ov.items():
                    if v: new_vals[k].add(v)
            return any(len(ex_vals[k]) == 1 and (ex_vals[k] | new_vals.get(k, set())) != ex_vals[k] for k in ex_vals)
        needs_refresh = bool(options_to_add) or _axis_becomes_multi()

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

        # Altid gem scrape-derived option-vaerdier for eksisterende SKUs
        # (bruges af productSet-baseret merge naar options aendres / renames).
        existing_variant_options = {}
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

        # TITEL-REGEN: kun når en NY akse tilføjes (needs_refresh) kan den delte titel
        # være blevet forkert (fx enkelt-farvet produkt får 2. farve → farven skal UD).
        # Regenerér fra hele det merged option-sæt via samme motor som create/orakel.
        if needs_refresh:
            try:
                merged_axis = defaultdict(set)
                for ov in list(all_variant_map.values()) + list(variant_map.values()):
                    for k, v in ov.items():
                        if v: merged_axis[k].add(v)
                t_opts = {k: sorted(vs) for k, vs in merged_axis.items() if len(vs) > 1}
                all_skus = list(existing_skus) + list(variant_map.keys())
                rep = next((feed_by_sku[s] for s in all_skus if s in feed_by_sku), first)
                rep_title = str(rep['Title']) if pd.notna(rep.get('Title')) else ''
                fcs = [str(feed_by_sku[s]['Color']).strip() for s in all_skus
                       if s in feed_by_sku and pd.notna(feed_by_sku[s].get('Color'))
                       and str(feed_by_sku[s]['Color']).strip()]
                fcs = fcs if len({c.lower() for c in fcs}) > 1 else []
                rpt = str(rep['Category']).split(' > ')[-1].strip() if pd.notna(rep.get('Category')) else ''
                nt, _ = generate_title(titlecase_feed(rep_title), t_opts, shared=True,
                                       product_type=rpt, feed_colors=fcs, n_variants=max(len(all_skus), 2))
                if nt and len(nt) >= 3:
                    nt = title_llm.repair_one(titlecase_feed(rep_title), nt, t_opts)  # LLM sidste lag
                    spec.regen_title = nt
                    print(f"   🏷️ Titel-regen (ny akse): {existing_handle} → {nt!r}")
            except Exception as e:
                print(f"   ⚠ Titel-regen fejl {existing_handle}: {str(e)[:100]}")

        # Add new variants — iterér i SCRAPE-order (variant_map.keys()), ikke feed-order
        feed_by_sku_local = {}
        for _, row in feed_rows.iterrows():
            s = normalize_sku(row['SKU'])
            if s and s not in feed_by_sku_local:
                feed_by_sku_local[s] = row

        for sku in variant_map.keys():
            if sku not in feed_by_sku_local:
                continue
            row = feed_by_sku_local[sku]
            try:
                opts = variant_map.get(sku, {})
                ordered = order_opts(opts)
                opts_dict = dict(ordered)
                all_images_var = get_all_images(row)
                raw_html_var = clean_vidaxl(row.get('HTML_description', ''))
                vspec = _row_to_variant_spec(
                    row, sku, opts_dict, set(), pricing_cfg,
                    is_first=False, all_images=all_images_var, raw_html=raw_html_var,
                    seed=existing_handle
                )
                vspec.option_values = ordered
                spec.new_variants.append(vspec)
            except Exception as e:
                print(f"   ⚠ Merge fejl SKU {sku}: {str(e)[:100]}")

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
        # Collect unique values per option i FIRST-SEEN order (= scrape-order =
        # vidaXL's naturlige display-order). Vi bruger IKKE sorted() fordi
        # alfabetisk sort fejler for dimensions ("100 cm" foer "30 cm") og
        # tal-strenge ("10" foer "2"). spec.variants iterer i scrape-order
        # (variant_map.keys()) per build_product_specs.
        option_values_seen = defaultdict(set)
        option_values_ordered = defaultdict(list)
        for v in spec.variants:
            for opt_name, opt_val in v.option_values:
                if opt_val not in option_values_seen[opt_name]:
                    option_values_seen[opt_name].add(opt_val)
                    option_values_ordered[opt_name].append(opt_val)
        product_options = [
            {"name": opt, "values": [{"name": val} for val in option_values_ordered[opt]]}
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

    # Shopify haard graense: max 250 entries pr. input-array i productSet.
    # Belt-and-braces (large-flowet capper normalt foer vi naar hertil): trunker
    # files og drop variant-file-refs der roeg ud — variant.file.originalSource
    # SKAL staa i files-arrayet, ellers fejler kaldet.
    if len(files) > 250:
        kept_urls = {f["originalSource"] for f in files[:250]}
        dropped = 0
        for vi in variants:
            vf = vi.get("file")
            if vf and vf["originalSource"] not in kept_urls:
                del vi["file"]
                dropped += 1
        print(f"    ⚠ {len(files)} files > Shopify max 250 — trunkerer ({dropped} variant-billeder droppes, kan repareres via repair_variant_data)")
        files = files[:250]

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


def _call_merge_via_productset_schema_replace(merge: MergeSpec, product_id: str,
                                                location_id: str,
                                                scrape_options_set: set,
                                                options_to_remove: list) -> dict:
    """Schema-rename merge via productSet (atomic).

    Bruges naar vidaXL har omstruktureret options (fx Bredde+Hoejde -> Stoerrelse).
    Produktets schema REPLACES med scrape's schema. Eksisterende variants
    remappes via SKU-match i merge.existing_variant_options.

    Hvis en eksisterende SKU IKKE er i scrape-data, kan vi ikke remappe den
    sikkert — vi skipper hele merge'en for at undgaa data-tab.
    """
    # 1. Fetch eksisterende variants
    d_existing = gql(FETCH_EXISTING_PRODUCT_STATE, {"id": product_id})
    p = d_existing.get('data', {}).get('product')
    if not p:
        raise Exception(f"Kunne ikke hente eksisterende produkt {product_id}")
    existing_variants = [e['node'] for e in p['variants']['edges']]

    # Verify alle eksisterende SKUs er i scrape-data
    missing_skus = [ev['sku'] for ev in existing_variants
                    if ev['sku'] and ev['sku'] not in merge.existing_variant_options]
    if missing_skus:
        print(f"    ⚠ SCHEMA-RENAME: {len(missing_skus)} eksisterende SKUs mangler i scrape-data ({missing_skus[:3]}...)")
        print(f"    ⏸  SKIP schema-replace — risiko for data-tab")
        skipped_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    '..', 'output', 'skipped_options_to_add.json')
        os.makedirs(os.path.dirname(skipped_path), exist_ok=True)
        existing = {}
        if os.path.exists(skipped_path):
            try:
                with open(skipped_path, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
            except: existing = {}
        existing[merge.existing_handle] = {
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "case": "schema-rename",
            "options_to_remove": options_to_remove,
            "scrape_options": sorted(scrape_options_set),
            "missing_skus_in_scrape": missing_skus,
            "reason": "Schema-rename ville miste data for SKUs ikke i scrape",
        }
        with open(skipped_path, 'w', encoding='utf-8') as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        return {"productVariants": [], "userErrors": [], "_skipped": True}

    # Brug scrape's option-rækkefølge (rækkefølge fra first sku's scrape opts)
    # Vi sikrer at "Title" droppes (hvis det var i existing) — vi har real options nu
    full_options = []
    for sku, opts in merge.existing_variant_options.items():
        for ov_name, _ov_val in opts:
            if ov_name not in full_options:
                full_options.append(ov_name)
        break  # første SKU's order er nok — alle skulle have samme schema fra scrape

    # Hvis nye varianter introducerer extra options (sjældent), tilfoej dem
    for v in merge.new_variants:
        for ov_name, _ov_val in v.option_values:
            if ov_name not in full_options:
                full_options.append(ov_name)

    if len(full_options) > 3:
        print(f"    ⏸  SKIP schema-replace: {len(full_options)} options > Shopify max 3 ({full_options})")
        return {"productVariants": [], "userErrors": [], "_skipped": True}

    print(f"    🔄 SCHEMA-RENAME via productSet: drop {options_to_remove} → {full_options}")

    # Upload nye variant-billeder
    unique_new_urls = []
    seen = set()
    for v in merge.new_variants:
        if v.image_url and v.image_url not in seen:
            seen.add(v.image_url)
            unique_new_urls.append(v.image_url)

    url_to_media_id = {}
    if unique_new_urls:
        _hr = _media_headroom(product_id)
        if _hr <= 0:
            print("    ⏭  Produkt har allerede 250 media — springer billed-upload over"); unique_new_urls = []
        elif len(unique_new_urls) > _hr:
            print(f"    ⚠ Kun plads til {_hr} af {len(unique_new_urls)} billeder (250-grænse)"); unique_new_urls = unique_new_urls[:_hr]
    if unique_new_urls:
        media_input = [
            {"originalSource": url, "mediaContentType": "IMAGE",
             "alt": f"Variant billede ({i+1}/{len(unique_new_urls)})"}
            for i, url in enumerate(unique_new_urls)
        ]
        d_media = gql(PRODUCT_CREATE_MEDIA, {"productId": product_id, "media": media_input})
        media_res = d_media.get('data', {}).get('productCreateMedia', {})
        for url, m in zip(unique_new_urls, media_res.get('media') or []):
            url_to_media_id[url] = m['id']
        print(f"    📷 Uploadede {len(url_to_media_id)} variant-billeder")

    # Saml option-vaerdier i FIRST-SEEN (scrape) order
    option_values_seen = defaultdict(set)
    option_values_ordered = defaultdict(list)

    def _add_val(name, val):
        if name in full_options and val not in option_values_seen[name]:
            option_values_seen[name].add(val)
            option_values_ordered[name].append(val)

    # Eksisterende SKUs i scrape-rækkefølge
    for sku, opts in merge.existing_variant_options.items():
        for ov_name, ov_val in opts:
            _add_val(ov_name, ov_val)
    # Nye varianter
    for v in merge.new_variants:
        for ov_name, ov_val in v.option_values:
            _add_val(ov_name, ov_val)

    product_options_input = [
        {"name": opt, "values": [{"name": val} for val in option_values_ordered[opt]]}
        for opt in full_options
    ]

    # Build variants
    variants_input = []
    # Eksisterende — remappet til scrape's schema
    for ev in existing_variants:
        sku = ev.get('sku', '')
        scrape_opts = dict(merge.existing_variant_options.get(sku, []))
        opt_vals = []
        for opt_name in full_options:
            if opt_name in scrape_opts:
                opt_vals.append({"optionName": opt_name, "name": scrape_opts[opt_name]})
            elif option_values_ordered[opt_name]:
                # Fallback: foerste vaerdi i scrape-order
                opt_vals.append({"optionName": opt_name, "name": option_values_ordered[opt_name][0]})
        variants_input.append({"id": ev['id'], "optionValues": opt_vals})

    # Nye varianter
    for v in merge.new_variants:
        opts_dict = dict(v.option_values)
        opt_vals = []
        for opt_name in full_options:
            if opt_name in opts_dict:
                opt_vals.append({"optionName": opt_name, "name": opts_dict[opt_name]})
            elif option_values_ordered[opt_name]:
                opt_vals.append({"optionName": opt_name, "name": option_values_ordered[opt_name][0]})

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

    input_payload = {
        "id": product_id,
        "productOptions": product_options_input,
        "variants": variants_input,
    }
    if merge.regen_title:
        input_payload["title"] = merge.regen_title
    d = gql(PRODUCT_SET_MUTATION, {"input": input_payload, "synchronous": True})
    res = d['data']['productSet']
    user_errs = res.get('userErrors') or []
    if user_errs:
        return {"productVariants": [], "userErrors": user_errs}
    all_variants = (res.get('product') or {}).get('variants', {}).get('edges', [])
    new_skus_set = {v.sku for v in merge.new_variants}
    new_variants_created = [e['node'] for e in all_variants
                            if e['node']['sku'] in new_skus_set]
    print(f"    ✅ Schema-rename gennemfoert: {len(existing_variants)} eks. + {len(new_variants_created)} nye variants")
    return {"productVariants": new_variants_created, "userErrors": []}


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
        _hr = _media_headroom(product_id)
        if _hr <= 0:
            print("    ⏭  Produkt har allerede 250 media — springer billed-upload over"); unique_new_urls = []
        elif len(unique_new_urls) > _hr:
            print(f"    ⚠ Kun plads til {_hr} af {len(unique_new_urls)} billeder (250-grænse)"); unique_new_urls = unique_new_urls[:_hr]
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
    # Saml option-vaerdier i FIRST-SEEN order (= scrape-order = vidaXL's
    # naturlige display-order). Rækkefølge: scrape foerst (autoritær), saa
    # nye varianter (skulle matche scrape), saa fallback til Shopify's
    # nuvaerende selectedOptions.
    option_values_seen = defaultdict(set)
    option_values_ordered = defaultdict(list)

    def _add_val(name, val):
        if name in full_options and val not in option_values_seen[name]:
            option_values_seen[name].add(val)
            option_values_ordered[name].append(val)

    # 1. Scrape-data for eksisterende SKUs (autoritær order)
    for sku, opts in merge.existing_variant_options.items():
        for ov_name, ov_val in opts:
            _add_val(ov_name, ov_val)
    # 2. Nye varianter
    for v in merge.new_variants:
        for ov_name, ov_val in v.option_values:
            _add_val(ov_name, ov_val)
    # 3. Fallback: Shopify's nuvaerende selectedOptions (kun for options vi ikke
    # har scrape-data for)
    for ev in existing_variants:
        for so in ev['selectedOptions']:
            _add_val(so['name'], so['value'])

    product_options_input = [
        {"name": opt, "values": [{"name": val} for val in option_values_ordered[opt]]}
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
            # Prioritet: scrape > nuvaerende > foerste vaerdi (i scrape-order)
            if opt_name in scrape_opts:
                val = scrape_opts[opt_name]
            elif opt_name in current_opts:
                val = current_opts[opt_name]
            elif option_values_ordered[opt_name]:
                val = option_values_ordered[opt_name][0]
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
            elif option_values_ordered[opt_name]:
                # Variant har ikke det option — brug foerste vaerdi (i scrape-order)
                opt_vals.append({"optionName": opt_name, "name": option_values_ordered[opt_name][0]})

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
    if merge.regen_title:
        input_payload["title"] = merge.regen_title
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


def _norm_ov(s) -> str:
    """Normalisér option-navn/-værdi til combo-sammenligning: collapse whitespace +
    lowercase. Robust mod case/mellemrum-forskelle mellem scrape og Shopify."""
    return ' '.join(str(s).split()).lower()


def _existing_variant_combo_keys(product_id: str) -> set:
    """Sæt af frozenset((optionName, value)) for produktets NUVÆRENDE varianter
    (NORMALISERET). Bruges til at frafiltrere allerede-eksisterende option-kombos
    (undgår VARIANT_ALREADY_EXISTS ved gentagen/delvis merge)."""
    q = """
    query($id: ID!) { product(id: $id) {
      variants(first: 250) { edges { node { selectedOptions { name value } } } } } }
    """
    try:
        d = gql(q, {"id": product_id})
        keys = set()
        for e in d['data']['product']['variants']['edges']:
            keys.add(frozenset((_norm_ov(o['name']), _norm_ov(o['value'])) for o in e['node']['selectedOptions']))
        return keys
    except Exception as ex:
        print(f"    ⚠ kunne ikke hente eksisterende variant-kombos: {str(ex)[:120]}")
        return set()


def _media_headroom(product_id: str) -> int:
    """Hvor mange flere media kan tilføjes før Shopifys hårde grænse på 250/produkt."""
    q = "query($id: ID!) { product(id: $id) { mediaCount { count } } }"
    try:
        d = gql(q, {"id": product_id})
        cur = (d['data']['product'].get('mediaCount') or {}).get('count', 0)
        return max(0, 250 - int(cur))
    except Exception:
        return 250


def _color_option_name(options: list):
    """Find farve-option-navnet (case-insensitivt) blandt produktets options."""
    return next((o for o in options if str(o).lower() in ('farve', 'color', 'colour')), None)


def _color_to_media_map(product_id: str, color_opt: str) -> dict:
    """Map farve-værdi -> et eksisterende MediaImage-id på produktet (fra varianter
    der allerede har et billede). Bruges til at give nye/billedløse varianter et
    farve-korrekt billede UDEN at uploade nyt — afgørende når produktet er på 250-grænsen."""
    if not color_opt:
        return {}
    q = """
    query($id: ID!) { product(id: $id) { variants(first: 250) { edges { node {
      selectedOptions { name value }
      media(first: 1) { edges { node { ... on MediaImage { id } } } } } } } } }
    """
    try:
        d = gql(q, {"id": product_id})
        cmap = {}
        for e in d['data']['product']['variants']['edges']:
            n = e['node']
            col = next((o['value'] for o in n['selectedOptions'] if o['name'] == color_opt), None)
            med = n['media']['edges']
            mid = med[0]['node'].get('id') if med and med[0].get('node') else None
            if col and mid and col not in cmap:
                cmap[col] = mid
        return cmap
    except Exception as ex:
        print(f"    ⚠ kunne ikke hente farve→media: {str(ex)[:100]}")
        return {}


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

    # Frafiltrér nye varianter hvis option-kombinationen ALLEREDE findes på produktet.
    # Sker når en tidligere (delvis) merge allerede har tilføjet dem — ellers fejler
    # productVariantsBulkCreate med VARIANT_ALREADY_EXISTS_CHANGE_OPTION_VALUE og vælter
    # hele kørslen. (Kombo-keys er option-space-specifikke, så options_to_add påvirkes ikke.)
    _existing_keys = _existing_variant_combo_keys(product_id)
    if _existing_keys:
        _before = len(merge.new_variants)
        merge.new_variants = [
            v for v in merge.new_variants
            if frozenset((_norm_ov(n), _norm_ov(val)) for n, val in v.option_values) not in _existing_keys
        ]
        _dropped = _before - len(merge.new_variants)
        if _dropped:
            print(f"    ⏭  Sprang {_dropped} variant(er) over — option-kombo findes allerede på produktet")
        if not merge.new_variants:
            print(f"    ✅ Alle {_before} varianter findes allerede — intet at merge")
            return {"productVariants": [], "userErrors": [], "_skipped": True}

    cur_options = fetch_product_options(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, merge.existing_handle)

    # Beregn schema-aendring:
    #   scrape_options_set: alle options scrape detekterer (fra new + existing
    #     variant_map data)
    #   options_to_remove: eksisterende options i Shopify som IKKE er i scrape
    #
    # Case A — kun options_to_add, ingen real options_to_remove:
    #   Eksisterende options beholdes + nye tilfoejes. (Title er "default" og
    #   regnes ikke som real existing.)
    #
    # Case B — schema-rename: options_to_remove indeholder REAL options.
    #   Det betyder vidaXL har aendret schema (fx Bredde+Hoejde konsolideret
    #   til Stoerrelse). Vi REPLACER produktets schema med scrape's.
    scrape_options_set = set(merge.options_to_add)
    for sku, opts in merge.existing_variant_options.items():
        for ov_name, _ov_val in opts:
            scrape_options_set.add(ov_name)
    for v in merge.new_variants:
        for ov_name, _ov_val in v.option_values:
            scrape_options_set.add(ov_name)
    real_existing = [o for o in cur_options if o != 'Title']
    options_to_remove = [o for o in real_existing if o not in scrape_options_set]

    if options_to_remove:
        # CASE B: schema-rename. Bygger fuldt-replaceret schema fra scrape.
        # Eksisterende variants remappes via SKU-match i existing_variant_options.
        return _call_merge_via_productset_schema_replace(
            merge, product_id, location_id, scrape_options_set, options_to_remove)

    full_options = list(cur_options) + [o for o in merge.options_to_add if o not in cur_options]

    # CASE A: Hvis nye options skal tilfoejes til eksisterende schema: brug
    # productSet-baseret merge. productSet er ATOMIC og haandterer
    # option-tilfoejelse + variant-opdatering + variant-creation i een enkelt
    # mutation. Det undgaar half-mutation-bugget vi havde med
    # productOptionsCreate + productVariantsBulkCreate.
    if merge.options_to_add:
        # Shopify har et HARD limit paa 3 options per produkt — skip hvis vi
        # ville overskride.
        if len(full_options) > 3:
            print(f"    ⏸  SKIP: produkt ville faa {len(full_options)} options ({full_options}) — Shopify max 3.")
            skipped_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                        '..', 'output', 'skipped_options_to_add.json')
            os.makedirs(os.path.dirname(skipped_path), exist_ok=True)
            existing = {}
            if os.path.exists(skipped_path):
                try:
                    with open(skipped_path, 'r', encoding='utf-8') as f:
                        existing = json.load(f)
                except: existing = {}
            existing[merge.existing_handle] = {
                "logged_at": datetime.now(timezone.utc).isoformat(),
                "options_attempted": full_options,
                "new_variant_skus": [v.sku for v in merge.new_variants],
                "reason": f"Shopify hard limit: max 3 options per product ({len(full_options)} forsoegt)",
            }
            with open(skipped_path, 'w', encoding='utf-8') as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)
            return {"productVariants": [], "userErrors": [], "_skipped": True}
        return _call_merge_via_productset(merge, product_id, full_options, location_id)

    # Upload alle unikke variant-billeder til produktet FOERST.
    # Vi bruger productCreateMedia og gemmer URL -> mediaId for at kunne
    # referere via mediaId i productVariantsBulkCreate (mediaSrc-feltet
    # tilfoejer kun media til produktet uden at linke det til varianten).
    # Farve-bevidst dækning: HVER variant skal have et variant-billede (bruges mange
    # steder i shoppen). Da der typisk er langt færre farver end varianter, prioriterer
    # vi ÉT billede pr. farve FØRST (coverage), så ekstra billeder (richness) op til 250.
    color_opt = _color_option_name(full_options)
    color_media = _color_to_media_map(product_id, color_opt) if color_opt else {}
    def _vcol(v): return dict(v.option_values).get(color_opt) if color_opt else None

    unique_image_urls = []
    seen = set(); colors_seen = set()
    # 1) coverage-pass: ét nyt billede pr. farve der IKKE allerede har media på produktet
    for v in merge.new_variants:
        if not v.image_url or v.image_url in seen:
            continue
        c = _vcol(v)
        if color_opt and c is not None and c not in color_media and c not in colors_seen:
            colors_seen.add(c); seen.add(v.image_url); unique_image_urls.append(v.image_url)
    # 2) richness-pass: resten af de unikke billeder
    for v in merge.new_variants:
        if v.image_url and v.image_url not in seen:
            seen.add(v.image_url); unique_image_urls.append(v.image_url)

    # Respektér Shopifys hårde grænse på 250 media/produkt — ellers fejler
    # productCreateMedia med PRODUCT_MEDIA_LIMIT_EXCEEDED.
    if unique_image_urls:
        _headroom = _media_headroom(product_id)
        if _headroom <= 0:
            print(f"    ⏭  Produkt har allerede 250 media — springer billed-upload over")
            unique_image_urls = []
        elif len(unique_image_urls) > _headroom:
            print(f"    ⚠ Kun plads til {_headroom} af {len(unique_image_urls)} nye billeder (250-grænse) — uploader {_headroom}")
            unique_image_urls = unique_image_urls[:_headroom]

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

    # Fyld farve→media med de nye uploads (så farver uden eksisterende media nu er dækket)
    for v in merge.new_variants:
        c = _vcol(v); mid = url_to_media_id.get(v.image_url)
        if color_opt and c is not None and mid and not color_media.get(c):
            color_media[c] = mid

    variants_input = []
    _covered = _fallback = _nocover = 0
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
        # Variant-image: eget uploadet billede hvis muligt, ellers FARVE-FALLBACK
        # (eksisterende/søskende-media i samme farve) — så HVER variant får et
        # farve-korrekt billede, selv når produktet er på 250-media-grænsen.
        mid = url_to_media_id.get(v.image_url)
        if mid:
            _covered += 1
        elif color_opt:
            mid = color_media.get(_vcol(v))
            if mid: _fallback += 1
            else: _nocover += 1
        else:
            _nocover += 1
        if mid:
            var_in["mediaId"] = mid
        var_in = {k: vv for k, vv in var_in.items() if vv is not None}
        variants_input.append(var_in)
    print(f"    🎨 Variant-billed-dækning: {_covered} eget + {_fallback} farve-fallback, {_nocover} uden ({len(merge.new_variants)} i alt)")

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
             "errors": 0, "already_exists_skipped": 0, "products": [], "merges": []}

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
                reorder_options_priority(gql, p['id'])   # Farve = option 1 (variant-thumbnail-swatch)
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
            # "already exists" = benign (variant-kombo findes allerede, fx ved option-
            # tilføjelse hvor eksisterende variant får default-værdi) → IKKE en run-fejl.
            benign = bool(errs) and all('already exist' in str(e.get('message', '')).lower() for e in errs)
            if errs and not benign:
                stats["errors"] += 1
                print(f"  [{i}] ❌ {merge.existing_handle}: {errs[:2]}")
            elif benign:
                stats["already_exists_skipped"] += 1
                print(f"  [{i}] ⏭ {merge.existing_handle}: variant-kombo findes allerede (benign — ikke en fejl)")
            else:
                created = res.get('productVariants') or []
                stats["merged_products"] += 1
                stats["merged_variants"] += len(created)
                stats["merges"].append({
                    "handle": merge.existing_handle,
                    "new_variant_count": len(created),
                })
                print(f"  [{i}] ✅ {merge.existing_handle}: +{len(created)} variants")
                # keeper-variant først + 1. variant sku-only + Farve = option 1 (konsistent med merge_executor)
                _kpid = find_product_by_handle(merge.existing_handle)
                if _kpid:
                    # option-reorder FØRST (re-sorterer varianter), keeper-først DEREFTER
                    reorder_options_priority(gql, _kpid)
                    reorder_keeper_first(gql, _kpid, merge.existing_skus)
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

    # Katalog Engine: per-product pricing config-lookup med caching
    # (én Supabase-call pr unik (vendor, product_type) kombo).
    _default_cfg = pricing.load_pricing_config()
    if not _default_cfg or not _default_cfg.get('tiers'):
        sys.exit("❌ Default pricing-config ikke loaded fra Supabase")

    # Lazy Supabase-client kun til pricing_rules-lookup
    try:
        from supabase import create_client as _sb_create
        _sb_client = _sb_create(os.environ.get('SUPABASE_URL'), os.environ.get('SUPABASE_SERVICE_KEY'))
    except Exception:
        _sb_client = None

    _pricing_cache = {}
    def resolve_pricing(vendor, product_type):
        key = (vendor or 'vidaXL', product_type or '__none__')
        if key not in _pricing_cache:
            cfg = pricing.load_pricing_config(_sb_client, vendor=vendor or 'vidaXL', product_type=product_type)
            _pricing_cache[key] = cfg or _default_cfg
        return _pricing_cache[key]

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
    # SKU→Link fra feedet (item_variant-metoden slår op pr. søster-SKU)
    links_by_sku = {}
    for _s, _lnk in zip(feed['SKU'].astype(str), feed['Link']):
        _n = normalize_sku(_s)
        if _n and _n not in links_by_sku and pd.notna(_lnk):
            links_by_sku[_n] = _lnk
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

        # Pålidelig metode: søster-SKUs fra mapping + item_variant pr. SKU.
        # Fallback til combo-scrape hvis mappingen ikke dækker masteren endnu.
        variant_map = fetch_variant_options_v2(scrape['master_pid'], scrape['options'], links_by_sku)
        if not variant_map or len(variant_map) < 2:
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
    product_specs = build_product_specs(product_groups, config, underkat, rum_dict, all_handles, feed, resolve_pricing)
    merge_specs = build_merge_specs(product_groups, config, underkat, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, feed, resolve_pricing)
    print(f"📋 Pricing-configs anvendt: {len(_pricing_cache)} unikke (vendor, type) kombinationer")

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
        ae_str = f", {stats.get('already_exists_skipped', 0)} already-exists skipped" if stats.get('already_exists_skipped') else ""
        print(f"\n📊 STATS: {stats['created_products']} created, "
              f"{stats['merged_products']} merged ({stats['merged_variants']} new variants), "
              f"{stats['errors']} errors{ae_str}{skip_str}")

        # Indsæt warmup state for nye SKUs — brug per-product pricing-config
        # saa sale_price fra warmup matcher Katalog Engine-regler.
        #
        # MODE-BEVIDST: warmup/rotation-state gælder KUN real_discount (Omnibus
        # A/B/C). Fictive_discount-produkter er altid på tilbud (fast markup +
        # fiktiv førpris sat ved oprettelse) og hører IKKE til i rotations-
        # maskinen — så vi skriver ikke vidaxl_pricing_state for dem.
        state_records = []
        skipped_fictive = 0
        warmup_until = (datetime.now(timezone.utc) + timedelta(days=WARMUP_DAYS)).isoformat()
        for spec in product_specs:
            _spec_cfg = resolve_pricing(spec.vendor, spec.product_type)
            if (_spec_cfg or {}).get('mode') == 'fictive_discount':
                skipped_fictive += len(spec.variants)
                continue
            for v in spec.variants:
                state_records.append({
                    'sku': v.sku,
                    'pricing_group': pricing.assign_group(v.sku),
                    'status': 'warmup',
                    'b2b_cost': v.cost,
                    'normal_price': v.price,
                    'sale_price': pricing.calculate_sale_price(v.cost, _spec_cfg),
                    'warmup_complete_at': warmup_until,
                })
        # Merges er vidaXL-produkter (dette repo scraper vidaXL) — spring warmup
        # over hvis vidaXL er fictive.
        _merge_fictive = (resolve_pricing('vidaXL', None) or {}).get('mode') == 'fictive_discount'
        for merge in merge_specs:
            if _merge_fictive:
                skipped_fictive += len(merge.new_variants)
                continue
            # For merge: vi har ikke product_type direkte paa MergeSpec, brug default
            # config. sale_price genberegnes alligevel ved naeste sync_prices_v2 run
            # med korrekt per-product config.
            for v in merge.new_variants:
                state_records.append({
                    'sku': v.sku,
                    'pricing_group': pricing.assign_group(v.sku),
                    'status': 'warmup',
                    'b2b_cost': v.cost,
                    'normal_price': v.price,
                    'sale_price': pricing.calculate_sale_price(v.cost, _default_cfg),
                    'warmup_complete_at': warmup_until,
                })
        if skipped_fictive:
            print(f"ℹ️  Sprang warmup/rotation-state over for {skipped_fictive} fictive variant(er) (altid på tilbud, ingen rotation)")
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
