"""Direct-API replacement for delete_products.py.

GENBRUGER 100% analyze-logikken fra v1 (find SKUs i Shopify som ikke
laengere er i VidaXL main-feed, gruppér per produkt for full-vs-variant
delete-strategi). Forskel:
  - OLD: writes matrixify_delete.csv → Matrixify importerer → Shopify
  - NEW: writes JSON med to-delete-list, og --apply mode kalder
         productDelete + productVariantsBulkDelete direkte.

WORKFLOW-STRUKTUR (3 jobs, samme som v1):
  1. analyze:   python delete_products_v2.py
                → writes output/delete_list.json + GitHub-outputs
  2. approval:  (uændret — trstringer/manual-approval@v1 gates på count)
  3. deploy:    python delete_products_v2.py --apply
                → reads delete_list.json + calls Shopify API

Threshold-gaten er bevaret: hvis delete_count > DELETE_THRESHOLD,
sættes needs_approval=true → workflow blokerer indtil manuelt
approve.

VEDVARENHEDSREGEL (tilføjet 2026-11-20)
  Feedet fra vidaXL er ustabilt: et par hundrede SKUs falder ud og
  kommer tilbage igen fra dag til dag. Målt 13.-20. nov 2026 svingede
  "mangler i feed" mellem 2.214 og 2.600, hvor kun 1.218 manglede ALLE
  8 dage. Slettes der på dagsbilledet, mister vi varer der stadig
  findes hos vidaXL — permanent.

  Derfor: en SKU slettes først når den har manglet i feedet
  PERSIST_DAYS dage I TRAEK. Streaks gemmes i output/missing_history.json,
  som analyze-jobbet committer hver dag. Kommer en SKU tilbage i feedet,
  nulstilles dens streak helt.

  Desuden en vagt mod katastrofalt afkortet feed: falder feedet mere end
  FEED_SHRINK_ABORT (default 25 %) ift. sidste kørsel, afbrydes der helt
  uden at skrive en sletteplan. En delvist hentet eller ødelagt CSV ser
  ellers ud som om titusindvis af varer er udgået — og pandas springer
  defekte linjer stille over (on_bad_lines='skip').
"""
import argparse
import io
import json
import os
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests


FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
DELETE_THRESHOLD = int(os.environ.get('DELETE_THRESHOLD', '1000'))
PERSIST_DAYS = int(os.environ.get('PERSIST_DAYS', '3'))
FEED_SHRINK_ABORT = float(os.environ.get('FEED_SHRINK_ABORT', '0.25'))

OUTPUT_JSON = "output/delete_list.json"
OUTPUT_CSV_LEGACY = "output/matrixify_delete.csv"
HISTORY_JSON = "output/missing_history.json"

VENDOR_FILTER = "vidaxl"


def normalize_sku(sku):
    if pd.isna(sku): return ''
    return str(sku).strip().replace('.0', '')


def fetch_feed_data(url):
    """Hent og udpak vidaXL main-feed (CSV i ZIP)."""
    print(f"\n📥 Henter main feed (ZIP)...")
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    print(f"   Download: {len(r.content) / 1024 / 1024:.1f} MB")
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files: raise Exception("Ingen CSV i ZIP")
        print(f"   Udpakker: {csv_files[0]}")
        with zf.open(csv_files[0]) as f:
            df = pd.read_csv(f, encoding='utf-8', on_bad_lines='skip')
    return df


def load_history():
    """Laes streak-historik. Fejler aldrig haardt — tom historik betyder
    blot at ingen SKU er moden til sletning i dag."""
    if not os.path.exists(HISTORY_JSON):
        print(f"ℹ  {HISTORY_JSON} findes ikke endnu — foerste koersel, intet slettes")
        return {}, None
    try:
        with open(HISTORY_JSON, encoding='utf-8') as f:
            d = json.load(f)
        return d.get('skus', {}), d.get('feed_count')
    except Exception as e:
        print(f"⚠ Kunne ikke laese {HISTORY_JSON} ({e}) — starter historikken forfra")
        return {}, None


def update_history(hist, missing_today, today):
    """Opdater streak pr. SKU.

    - manglede i gaar (eller allerede registreret i dag) → streak +1
    - ellers → ny streak paa 1
    - SKUs der IKKE mangler i dag fjernes helt (de er tilbage i feedet)
    """
    yday = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    new = {}
    for sku in missing_today:
        prev = hist.get(sku)
        if prev and prev.get('last') == today:
            new[sku] = dict(prev)
        elif prev and prev.get('last') == yday:
            new[sku] = {'first': prev.get('first', today), 'last': today,
                        'days': int(prev.get('days', 1)) + 1}
        else:
            new[sku] = {'first': today, 'last': today, 'days': 1}
    returned = set(hist.keys()) - set(missing_today)
    return new, returned


def save_history(hist, today, feed_count):
    os.makedirs("output", exist_ok=True)
    with open(HISTORY_JSON, 'w', encoding='utf-8') as f:
        json.dump({'updated_at': today, 'persist_days': PERSIST_DAYS,
                   'feed_count': feed_count, 'skus': hist},
                  f, separators=(',', ':'), sort_keys=True)


def gql(query, variables=None):
    if not SHOPIFY_ACCESS_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
    headers = {'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN, 'Content-Type': 'application/json'}
    payload = {'query': query}
    if variables: payload['variables'] = variables
    for attempt in range(1, 5):
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        d = r.json()
        if 'errors' in d:
            if any('Throttled' in str(e) for e in d['errors']) and attempt < 4:
                time.sleep(2 ** attempt); continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        cost = d.get('extensions', {}).get('cost', {}).get('throttleStatus', {})
        if cost.get('currentlyAvailable', 1000) < 200:
            time.sleep(0.5)
        return d
    raise Exception("Max retries exceeded")


def fetch_shopify_data(vendor_filter=VENDOR_FILTER):
    """Hent vidaXL-varianter fra Shopify med variant_id + product_id."""
    print(f"\n📥 Henter Shopify varianter (vendor={vendor_filter})...")
    skus = set()
    product_variants = defaultdict(set)
    sku_to_product = {}
    sku_to_variant = {}

    cursor = None
    total = 0
    vendor_matched = 0
    skipped_vendors = set()

    while True:
        q = """
        query getVariants($cursor: String) {
          productVariants(first: 250, after: $cursor) {
            edges {
              cursor
              node {
                id
                sku
                product { id vendor }
              }
            }
            pageInfo { hasNextPage }
          }
        }
        """
        d = gql(q, {'cursor': cursor})
        variants = d['data']['productVariants']
        edges = variants['edges']

        for edge in edges:
            n = edge['node']
            sku = n.get('sku') or ''
            p = n.get('product') or {}
            vendor = (p.get('vendor') or '').lower()
            product_id_gid = p.get('id') or ''
            variant_id_gid = n.get('id') or ''

            if vendor == vendor_filter.lower():
                if sku:
                    norm = normalize_sku(sku)
                    skus.add(norm)
                    product_variants[product_id_gid].add(norm)
                    sku_to_product[norm] = product_id_gid
                    sku_to_variant[norm] = variant_id_gid
                    vendor_matched += 1
            elif vendor:
                skipped_vendors.add(vendor)

        total += len(edges)
        if total % 5000 == 0 or not variants['pageInfo']['hasNextPage']:
            print(f"   Scanned {total:,} variants, found {len(skus):,} vidaXL SKUs")
        if not variants['pageInfo']['hasNextPage']: break
        cursor = edges[-1]['cursor']

    print(f"✅ {len(skus):,} vidaXL SKUs ({len(product_variants):,} unikke produkter)")
    return skus, product_variants, sku_to_product, sku_to_variant


def compute_deletes(to_delete_skus, product_variants, sku_to_product, sku_to_variant):
    """Decide: full-product-delete vs variant-only-delete."""
    products_affected = defaultdict(set)
    for sku in to_delete_skus:
        pid = sku_to_product.get(sku)
        if pid:
            products_affected[pid].add(sku)

    full_product_deletes = []
    variant_deletes = defaultdict(list)
    full_count = 0
    variant_count = 0

    for product_id, skus_to_delete in products_affected.items():
        all_skus_on_product = product_variants.get(product_id, set())
        if skus_to_delete >= all_skus_on_product:
            full_product_deletes.append(product_id)
            full_count += 1
        else:
            for sku in skus_to_delete:
                vid = sku_to_variant.get(sku)
                if vid:
                    variant_deletes[product_id].append(vid)
                    variant_count += 1

    unmapped = to_delete_skus - set(sku_to_product.keys())
    if unmapped:
        print(f"⚠ {len(unmapped)} SKUs uden product mapping — kan ikke slettes")

    return {
        "full_product_deletes": full_product_deletes,
        "variant_deletes": dict(variant_deletes),
        "stats": {
            "full_product_count": full_count,
            "variant_count": variant_count,
            "unmapped_count": len(unmapped),
        },
    }


def run_analyze():
    if not FEED_URL or not SHOPIFY_STORE or not SHOPIFY_ACCESS_TOKEN:
        sys.exit("❌ FEED_URL / SHOPIFY_STORE / SHOPIFY_ACCESS_TOKEN mangler")

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist, prev_feed_count = load_history()

    feed_df = fetch_feed_data(FEED_URL)
    feed_df['SKU'] = feed_df['SKU'].apply(normalize_sku)
    feed_skus = set(feed_df['SKU'].unique())
    feed_skus.discard('')
    print(f"✅ {len(feed_df):,} produkter i feed, {len(feed_skus):,} unikke SKUs")

    # --- vagt: katastrofalt afkortet feed ---
    if prev_feed_count:
        drop = 1.0 - (len(feed_skus) / float(prev_feed_count))
        print(f"   Feed-aendring siden sidst: {prev_feed_count:,} → {len(feed_skus):,} "
              f"({drop * -100:+.1f} %)")
        if drop > FEED_SHRINK_ABORT:
            sys.exit(f"❌ AFBRUDT: feedet er faldet {drop * 100:.1f} % "
                     f"(graense {FEED_SHRINK_ABORT * 100:.0f} %). "
                     f"Sandsynligvis afkortet eller defekt download — "
                     f"ingen sletteplan skrevet, historikken er uaendret.")

    shop_skus, product_variants, sku_to_product, sku_to_variant = fetch_shopify_data()
    missing = shop_skus - feed_skus

    # --- vedvarenhed ---
    hist, returned = update_history(hist, missing, today)
    save_history(hist, today, len(feed_skus))

    eligible = set(s for s, v in hist.items() if int(v.get('days', 0)) >= PERSIST_DAYS)
    waiting = missing - eligible

    print(f"\n📊 Resultat:")
    print(f"   - vidaXL produkter i Shopify: {len(shop_skus):,}")
    print(f"   - SKUs i feed:               {len(feed_skus):,}")
    print(f"   - mangler i feed i dag:      {len(missing):,}")
    print(f"\n🕒 Vedvarenhed (kraever {PERSIST_DAYS} dage i traek):")
    print(f"   - kom tilbage i feedet:      {len(returned):,}  (streak nulstillet)")
    print(f"   - MODNE til sletning:        {len(eligible):,}")
    print(f"   - afventer flere dage:       {len(waiting):,}")

    plan = compute_deletes(eligible, product_variants, sku_to_product, sku_to_variant)
    print(f"\n📋 Plan:")
    print(f"   - Full product deletes:  {plan['stats']['full_product_count']}")
    print(f"   - Variant-only deletes:  {plan['stats']['variant_count']}")
    print(f"   - Unmapped (skipped):    {plan['stats']['unmapped_count']}")

    delete_count = len(eligible)
    needs_approval = "true" if delete_count > DELETE_THRESHOLD else "false"
    print(f"\n{'⚠️ THRESHOLD OVERSKREDET' if delete_count > DELETE_THRESHOLD else '✅ Under threshold'}: "
          f"{delete_count:,} {'>' if delete_count > DELETE_THRESHOLD else '≤'} {DELETE_THRESHOLD:,}")

    os.makedirs("output", exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "delete_count": delete_count,
        "missing_count": len(missing),
        "waiting_count": len(waiting),
        "returned_count": len(returned),
        "persist_days": PERSIST_DAYS,
        "feed_count": len(feed_skus),
        "shopify_count": len(shop_skus),
        "needs_approval": needs_approval == "true",
        "plan": plan,
    }
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(payload, f, separators=(',', ':'))
    print(f"💾 delete_list.json skrevet ({delete_count:,} modne SKUs)")

    pd.DataFrame({'Command': [], 'Variant SKU': [], 'Variant Command': []}) \
        .to_csv(OUTPUT_CSV_LEGACY, index=False, encoding='utf-8-sig', sep=',')

    gh_out = os.environ.get('GITHUB_OUTPUT', '')
    if gh_out:
        with open(gh_out, 'a') as f:
            f.write(f"delete_count={delete_count}\n")
            f.write(f"missing_count={len(missing)}\n")
            f.write(f"waiting_count={len(waiting)}\n")
            f.write(f"returned_count={len(returned)}\n")
            f.write(f"persist_days={PERSIST_DAYS}\n")
            f.write(f"needs_approval={needs_approval}\n")
            f.write(f"feed_count={len(feed_skus)}\n")
            f.write(f"shopify_count={len(shop_skus)}\n")
    print("✅ Færdig (analyze)")


PRODUCT_DELETE = """
mutation productDelete($input: ProductDeleteInput!) {
  productDelete(input: $input) {
    deletedProductId
    userErrors { field message }
  }
}
"""

VARIANTS_BULK_DELETE = """
mutation productVariantsBulkDelete($productId: ID!, $variantsIds: [ID!]!) {
  productVariantsBulkDelete(productId: $productId, variantsIds: $variantsIds) {
    product { id }
    userErrors { field message }
  }
}
"""


def run_apply():
    if not os.path.exists(OUTPUT_JSON):
        sys.exit(f"❌ {OUTPUT_JSON} findes ikke — koer --analyze foerst")

    with open(OUTPUT_JSON, encoding='utf-8') as f:
        payload = json.load(f)

    plan = payload['plan']
    full_deletes = plan['full_product_deletes']
    variant_deletes = plan['variant_deletes']

    print(f"🚀 APPLY: {len(full_deletes)} full product-deletes + "
          f"{sum(len(v) for v in variant_deletes.values())} variant-deletes "
          f"i {len(variant_deletes)} produkter")

    stats = {"products_deleted": 0, "variants_deleted": 0, "errors": 0}

    for n, product_id in enumerate(full_deletes, 1):
        try:
            d = gql(PRODUCT_DELETE, {"input": {"id": product_id}})
            errs = d['data']['productDelete']['userErrors']
            if errs:
                stats["errors"] += 1
                print(f"  ⚠ {product_id}: {errs[:1]}")
            else:
                stats["products_deleted"] += 1
                if n % 50 == 0 or n == len(full_deletes):
                    print(f"  Product deletes: {n}/{len(full_deletes)}")
        except Exception as e:
            stats["errors"] += 1
            print(f"  ❌ {product_id}: {str(e)[:150]}")

    products_with_variants = list(variant_deletes.items())
    for n, (product_id, variant_ids) in enumerate(products_with_variants, 1):
        try:
            d = gql(VARIANTS_BULK_DELETE, {
                "productId": product_id,
                "variantsIds": variant_ids,
            })
            errs = d['data']['productVariantsBulkDelete']['userErrors']
            if errs:
                stats["errors"] += len(errs)
                print(f"  ⚠ {product_id}: {errs[:1]}")
            else:
                stats["variants_deleted"] += len(variant_ids)
                if n % 50 == 0 or n == len(products_with_variants):
                    print(f"  Variant deletes: {n}/{len(products_with_variants)} products")
        except Exception as e:
            stats["errors"] += len(variant_ids)
            print(f"  ❌ {product_id}: {str(e)[:150]}")

    print(f"\n📊 STATS: {stats}")
    if stats["errors"]:
        sys.exit(1)
    print("✅ Færdig (apply)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true',
                        help="Read delete_list.json + execute via Shopify API. "
                             "Default mode er analyze (skriv JSON, ingen Shopify).")
    args = parser.parse_args()

    print("VidaXL Delete Processor v2 — direct API")
    print("=" * 60)
    if args.apply:
        run_apply()
    else:
        run_analyze()


if __name__ == "__main__":
    main()
