"""Direct-API replacement for create_large_products.py (Dag 6).

Samme logik som v1: processerer "store" produkter (≥100 kombinationer) fra
skipped_large_products.json. Kan splitte enkelt produkt over flere dage
(pending → partial → done) via variant-budget tracking.

GENBRUGER spec-builders + apply-funktioner fra create_products_v2.py.

Modes:
  --dry-run (default): bygger ProductSpec[] + dumper JSON. Ingen Shopify.
  --live: aktiver Shopify-kald.
  --limit N: max N store produkter (default fra MAX_LARGE_PRODUCTS env, fallback 5).
"""
import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing
from product_utils import (
    WARMUP_DAYS, fetch_feed, fetch_shopify_data, fetch_variant_skus,
    load_config, normalize_sku, scrape_vidaxl, upsert_warmup_state,
)
# Genbrug create_products_v2 funktionalitet
from create_products_v2 import (
    apply_specs, build_merge_specs, build_product_specs,
    get_primary_location_id,
)


# === CONFIG ============================================================
FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
MAX_LARGE_PRODUCTS = int(os.environ.get('MAX_LARGE_PRODUCTS', '5'))
MAX_VARIANTS_HARD = 999
MIN_STOCK_VARIANT = 4
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           '..', 'config', 'Kategori_Config.xlsx')
SKIPPED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            '..', 'output', 'skipped_large_products.json')
COUNT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          '..', 'output', 'daily_variant_count.txt')
SPECS_DUMP_PATH = "output/large_product_specs_v2.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--limit', type=int, default=None,
                        help="Max store produkter at oprette (default MAX_LARGE_PRODUCTS=5)")
    args = parser.parse_args()

    missing = [n for n, v in [('FEED_URL', FEED_URL), ('SHOPIFY_STORE', SHOPIFY_STORE),
                              ('SHOPIFY_ACCESS_TOKEN', SHOPIFY_ACCESS_TOKEN)] if not v]
    if missing: sys.exit(f"❌ Manglende env: {', '.join(missing)}")

    mode = "LIVE" if args.live else "DRY-RUN"
    effective_limit = args.limit if args.limit is not None else MAX_LARGE_PRODUCTS
    print(f"🚀 create_large_products_v2 — {mode} (max {effective_limit} produkter)")

    # 1. Læs daglig variant count
    daily_used = 0
    if os.path.exists(COUNT_PATH):
        try:
            with open(COUNT_PATH, 'r') as f: daily_used = int(f.read().strip())
        except: daily_used = 0
    budget = MAX_VARIANTS_HARD - daily_used
    print(f"⚙️ Dagligt brugt: {daily_used}, Budget: {budget}")

    if budget <= 0:
        print("⚠️ Ingen variant-budget tilbage i dag!")
        sys.exit(0)

    # 2. Læs skipped liste
    if not os.path.exists(SKIPPED_PATH):
        print("⚠️ Ingen skipped-liste fundet")
        sys.exit(0)

    with open(SKIPPED_PATH, 'r', encoding='utf-8') as f:
        skipped = json.load(f)

    to_process = {k: v for k, v in skipped.items() if v.get('status') in ('pending', 'partial')}
    print(f"📋 {len(to_process)} store produkter at processere")

    if not to_process:
        print("✅ Ingen store produkter at processere")
        sys.exit(0)

    # 3. Hent feed + Shopify data
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

    # Katalog Engine: per-product pricing config-resolver (samme moenster
    # som create_products_v2.py)
    _default_cfg = pricing.load_pricing_config()
    if not _default_cfg or not _default_cfg.get('tiers'):
        sys.exit("❌ Default pricing-config ikke loaded fra Supabase")
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

    # 4. Process store produkter (samme logik som v1)
    print(f"\n🔍 Processerer store produkter...")
    product_groups = []
    total_variants = 0
    products_processed = 0
    sorted_items = sorted(to_process.items(),
                          key=lambda x: 0 if x[1]['status'] == 'partial' else 1)

    for pid, item in sorted_items:
        if products_processed >= effective_limit:
            print(f"   Max {effective_limit} produkter nået"); break
        if total_variants >= budget:
            print(f"   Budget opbrugt"); break
        remaining_budget = budget - total_variants
        print(f"\n📦 [{products_processed+1}] PID {pid} ({item['status']})")

        if item['status'] == 'pending':
            variant_map = item.get('variant_map')
            if not variant_map:
                options = item.get('options', {})
                if not options:
                    scrape = scrape_vidaxl(item['url'])
                    time.sleep(1)
                    if not scrape['success'] or not scrape['options']:
                        skipped[pid]['status'] = 'done'; continue
                    options = scrape['options']
                    skipped[pid]['options'] = options
                variant_map = fetch_variant_skus(pid, options)
                if not variant_map:
                    skipped[pid]['status'] = 'done'; continue
                skipped[pid]['variant_map'] = variant_map

            valid_skus = []
            for v_sku in variant_map:
                if v_sku in shopify_skus: continue
                if v_sku not in feed_by_sku: continue
                fr = feed_by_sku[v_sku]
                if float(fr.get('Stock', 0) or 0) >= MIN_STOCK_VARIANT and float(fr.get('B2B price', 0) or 0) > 0:
                    valid_skus.append(v_sku)
            if not valid_skus:
                skipped[pid]['status'] = 'done'; continue

            existing_handle = None
            existing_skus_for_group = []
            for v_sku in variant_map:
                if v_sku in shopify_skus:
                    existing_skus_for_group.append(v_sku)
                    if not existing_handle:
                        existing_handle = sku_to_handle.get(v_sku)

            if len(valid_skus) <= remaining_budget:
                take_skus = valid_skus
                skipped[pid]['status'] = 'done'
                skipped[pid]['created_skus'] = take_skus
                print(f"   ✅ Opretter alle {len(take_skus)} variants")
            else:
                take_skus = valid_skus[:remaining_budget]
                remaining = valid_skus[remaining_budget:]
                skipped[pid]['status'] = 'partial'
                skipped[pid]['created_skus'] = take_skus
                skipped[pid]['remaining_skus'] = remaining
                print(f"   ⚡ Partial: {len(take_skus)} nu, {len(remaining)} venter")

            take_variant_map = {s: variant_map[s] for s in take_skus if s in variant_map}
            if existing_handle:
                skipped[pid]['handle'] = existing_handle
                product_groups.append({
                    'feed_rows': feed[feed['SKU'].isin(take_skus)],
                    'variant_map': take_variant_map, 'options': item.get('options', {}),
                    'existing_handle': existing_handle, 'is_merge': True,
                    'existing_skus': existing_skus_for_group, 'all_variant_map': variant_map,
                })
            else:
                product_groups.append({
                    'feed_rows': feed[feed['SKU'].isin(take_skus)],
                    'variant_map': take_variant_map, 'options': item.get('options', {}),
                    'existing_handle': None, 'is_merge': False,
                    'existing_skus': [], 'all_variant_map': {},
                })
            total_variants += len(take_skus)

        elif item['status'] == 'partial':
            remaining_skus = item.get('remaining_skus', [])
            variant_map = item.get('variant_map', {})
            handle = item.get('handle')
            if not remaining_skus or not variant_map:
                skipped[pid]['status'] = 'done'; continue

            still_remaining = [s for s in remaining_skus
                              if s not in shopify_skus and s in feed_by_sku]
            if not still_remaining:
                skipped[pid]['status'] = 'done'; continue

            if not handle:
                for created_sku in item.get('created_skus', []):
                    if created_sku in sku_to_handle:
                        handle = sku_to_handle[created_sku]
                        skipped[pid]['handle'] = handle; break
            if not handle:
                print(f"   ⚠ Kan ikke finde handle for partial"); continue

            if len(still_remaining) <= remaining_budget:
                take_skus = still_remaining
                skipped[pid]['status'] = 'done'
                skipped[pid]['created_skus'].extend(take_skus)
                print(f"   ✅ Merger alle {len(take_skus)} resterende")
            else:
                take_skus = still_remaining[:remaining_budget]
                new_remaining = still_remaining[remaining_budget:]
                skipped[pid]['created_skus'].extend(take_skus)
                skipped[pid]['remaining_skus'] = new_remaining
                print(f"   ⚡ Partial merge: {len(take_skus)} nu, {len(new_remaining)} venter")

            take_variant_map = {s: variant_map[s] for s in take_skus if s in variant_map}
            existing_in_shopify = [s for s in item.get('created_skus', []) if s in shopify_skus]
            product_groups.append({
                'feed_rows': feed[feed['SKU'].isin(take_skus)],
                'variant_map': take_variant_map, 'options': item.get('options', {}),
                'existing_handle': handle, 'is_merge': True,
                'existing_skus': existing_in_shopify, 'all_variant_map': variant_map,
            })
            total_variants += len(take_skus)

        products_processed += 1

    # 5. Byg specs
    merges = sum(1 for g in product_groups if g['is_merge'])
    news = len(product_groups) - merges
    print(f"\n✅ {products_processed} processeret ({news} nye, {merges} merge), {total_variants} variants")

    product_specs = build_product_specs(product_groups, config, underkat, rum_dict,
                                        all_handles, feed, resolve_pricing)
    merge_specs = build_merge_specs(product_groups, config, underkat,
                                    SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, feed, resolve_pricing)
    print(f"📋 Pricing-configs anvendt: {len(_pricing_cache)} unikke (vendor, type) kombinationer")

    os.makedirs("output", exist_ok=True)
    with open(SPECS_DUMP_PATH, 'w', encoding='utf-8') as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": mode,
            "product_specs": [asdict(s) for s in product_specs],
            "merge_specs": [asdict(s) for s in merge_specs],
        }, f, default=str, indent=2)
    print(f"💾 Specs gemt: {SPECS_DUMP_PATH}")

    # 6. Apply hvis --live
    if args.live:
        location_id = get_primary_location_id()
        stats = apply_specs(product_specs, merge_specs, location_id, limit=None)  # limit allerede applied via products_processed
        print(f"\n📊 STATS: {stats['created_products']} created, "
              f"{stats['merged_products']} merged ({stats['merged_variants']} new variants), "
              f"{stats['errors']} errors")

        # Warmup state — per-product config via resolveren
        warmup_until = (datetime.now(timezone.utc) + timedelta(days=WARMUP_DAYS)).isoformat()
        state_records = []
        for spec in product_specs:
            _spec_cfg = resolve_pricing(spec.vendor, spec.product_type)
            for v in spec.variants:
                state_records.append({
                    'sku': v.sku, 'pricing_group': pricing.assign_group(v.sku),
                    'status': 'warmup', 'b2b_cost': v.cost,
                    'normal_price': v.price,
                    'sale_price': pricing.calculate_sale_price(v.cost, _spec_cfg),
                    'warmup_complete_at': warmup_until,
                })
        for merge in merge_specs:
            for v in merge.new_variants:
                state_records.append({
                    'sku': v.sku, 'pricing_group': pricing.assign_group(v.sku),
                    'status': 'warmup', 'b2b_cost': v.cost,
                    'normal_price': v.price,
                    'sale_price': pricing.calculate_sale_price(v.cost, _default_cfg),
                    'warmup_complete_at': warmup_until,
                })
        upsert_warmup_state(state_records)

        # Opdater daily variant count
        new_total = daily_used + total_variants
        with open(COUNT_PATH, 'w') as f: f.write(str(new_total))
        print(f"   💾 Variant count: {daily_used} + {total_variants} = {new_total}")

        # Opdater skipped liste (fjern done)
        skipped = {k: v for k, v in skipped.items() if v.get('status') != 'done'}
        with open(SKIPPED_PATH, 'w', encoding='utf-8') as f:
            json.dump(skipped, f, ensure_ascii=False, indent=2)
        print(f"   💾 Skipped liste: {len(skipped)} produkter tilbage")

        if stats["errors"]: sys.exit(1)
        print("\n✅ SUCCESS")
    else:
        print(f"\n[DRY-RUN] {len(product_specs)} new + {len(merge_specs)} merge specs bygget")

    gh = os.environ.get('GITHUB_OUTPUT', '')
    if gh:
        with open(gh, 'a') as f:
            f.write(f"large_variant_count={total_variants}\n")
            f.write(f"large_products={products_processed}\n")
            f.write(f"remaining_skipped={len(skipped)}\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ FATAL: {e}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)
