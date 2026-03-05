"""
VidaXL Product Creator v3.0 — STOR kørsel
Processerer produkter med 100+ kombinationer fra skipped_large_products.json.
Kan splitte store produkter over flere dage (partial → merge).
Respekterer daglig variant-grænse (999 - allerede oprettet).
"""
import pandas as pd
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from product_utils import *

print("VidaXL Product Creator v3.0 - STOR kørsel (100+ kombos)")
print("=" * 60)

# ============================================================
# KONFIGURATION
# ============================================================
FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
MAX_LARGE_PRODUCTS = int(os.environ.get('MAX_LARGE_PRODUCTS', '5'))
MAX_VARIANTS_HARD = 999
MIN_STOCK_VARIANT = 4
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'Kategori_Config.xlsx')
SKIPPED_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'output', 'skipped_large_products.json')
COUNT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'output', 'daily_variant_count.txt')

missing = []
if not FEED_URL: missing.append('FEED_URL')
if not SHOPIFY_STORE: missing.append('SHOPIFY_STORE')
if not SHOPIFY_ACCESS_TOKEN: missing.append('SHOPIFY_ACCESS_TOKEN')
if missing:
    print(f"❌ Manglende: {', '.join(missing)}")
    sys.exit(1)

try:
    # 1. Læs daglig variant count
    daily_used = 0
    if os.path.exists(COUNT_PATH):
        try:
            with open(COUNT_PATH, 'r') as f:
                daily_used = int(f.read().strip())
        except: daily_used = 0

    budget = MAX_VARIANTS_HARD - daily_used
    print(f"⚙️ Dagligt brugt: {daily_used}, Budget: {budget}, Max produkter: {MAX_LARGE_PRODUCTS}")

    if budget <= 0:
        print("⚠️ Ingen variant-budget tilbage i dag!")
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_large_new.xlsx')
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_large_merge.xlsx')
        sys.exit(0)

    # 2. Læs skipped liste
    if not os.path.exists(SKIPPED_PATH):
        print("⚠️ Ingen skipped-liste fundet")
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_large_new.xlsx')
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_large_merge.xlsx')
        sys.exit(0)

    with open(SKIPPED_PATH, 'r', encoding='utf-8') as f:
        skipped = json.load(f)

    # Filtrer kun pending og partial
    to_process = {k: v for k, v in skipped.items() if v.get('status') in ('pending', 'partial')}
    print(f"📋 {len(to_process)} store produkter at processere ({len(skipped)} total i listen)")

    if not to_process:
        print("✅ Ingen store produkter at processere!")
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_large_new.xlsx')
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_large_merge.xlsx')
        sys.exit(0)

    # 3. Hent data
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

    config, underkat, rum_dict = load_config(CONFIG_PATH)

    # 4. Process store produkter
    print(f"\n🔍 Processerer store produkter (budget: {budget})...")
    product_groups = []
    total_variants = 0
    products_processed = 0

    # Sortér: partial først (de er delvist færdige), derefter pending
    sorted_items = sorted(to_process.items(), key=lambda x: 0 if x[1]['status'] == 'partial' else 1)

    for pid, item in sorted_items:
        if products_processed >= MAX_LARGE_PRODUCTS:
            print(f"   Max {MAX_LARGE_PRODUCTS} produkter nået"); break
        if total_variants >= budget:
            print(f"   Budget opbrugt ({total_variants} ≥ {budget})"); break

        remaining_budget = budget - total_variants
        print(f"\n📦 [{products_processed+1}] PID {pid} (status: {item['status']}, budget: {remaining_budget})...")

        if item['status'] == 'pending':
            # === PENDING: Scrape og opret (eller start partial) ===
            variant_map = item.get('variant_map')

            if not variant_map:
                # Scrape varianter
                print(f"   Scraper {item['url']}...")
                # Brug cached options fra skipped-listen
                options = item.get('options', {})
                if not options:
                    scrape = scrape_vidaxl(item['url'])
                    time.sleep(1)
                    if not scrape['success'] or not scrape['options']:
                        print(f"   ⚠️ Scrape fejlede, springer over")
                        skipped[pid]['status'] = 'done'
                        continue
                    options = scrape['options']
                    skipped[pid]['options'] = options

                variant_map = fetch_variant_skus(pid, options)
                if not variant_map:
                    print(f"   ⚠️ Ingen varianter fundet")
                    skipped[pid]['status'] = 'done'
                    continue

                # Gem variant_map i skipped for caching
                skipped[pid]['variant_map'] = variant_map

            # Filtrer gyldige varianter
            valid_skus = []
            for v_sku, v_opts in variant_map.items():
                if v_sku in shopify_skus: continue
                if v_sku not in feed_by_sku: continue
                fr = feed_by_sku[v_sku]
                stock = float(fr.get('Stock', 0) or 0)
                price = float(fr.get('B2B price', 0) or 0)
                if stock >= MIN_STOCK_VARIANT and price > 0:
                    valid_skus.append(v_sku)

            if not valid_skus:
                print(f"   ⚠️ Ingen gyldige varianter")
                skipped[pid]['status'] = 'done'
                continue

            # Check om der er eksisterende varianter (merge?)
            existing_handle = None
            for v_sku in variant_map.keys():
                if v_sku in shopify_skus:
                    existing_handle = sku_to_handle.get(v_sku)
                    if existing_handle: break

            print(f"   {len(valid_skus)} gyldige varianter")

            # Tag hvad der er plads til
            if len(valid_skus) <= remaining_budget:
                # Alt passer — opret hele produktet
                take_skus = valid_skus
                skipped[pid]['status'] = 'done'
                skipped[pid]['created_skus'] = take_skus
                print(f"   ✅ Opretter alle {len(take_skus)} varianter")
            else:
                # Split — tag hvad der er plads til
                take_skus = valid_skus[:remaining_budget]
                remaining = valid_skus[remaining_budget:]
                skipped[pid]['status'] = 'partial'
                skipped[pid]['created_skus'] = take_skus
                skipped[pid]['remaining_skus'] = remaining
                print(f"   ⚡ Partial: opretter {len(take_skus)}, {len(remaining)} venter til i morgen")

            take_variant_map = {s: variant_map[s] for s in take_skus if s in variant_map}

            if existing_handle:
                # Merge til eksisterende
                skipped[pid]['handle'] = existing_handle
                product_groups.append({
                    'feed_rows': feed[feed['SKU'].isin(take_skus)],
                    'variant_map': take_variant_map,
                    'options': item.get('options', {}),
                    'existing_handle': existing_handle,
                    'is_merge': True
                })
            else:
                # Nyt produkt
                product_groups.append({
                    'feed_rows': feed[feed['SKU'].isin(take_skus)],
                    'variant_map': take_variant_map,
                    'options': item.get('options', {}),
                    'existing_handle': None,
                    'is_merge': False
                })

            total_variants += len(take_skus)

        elif item['status'] == 'partial':
            # === PARTIAL: Brug cached data, merge resterende ===
            remaining_skus = item.get('remaining_skus', [])
            variant_map = item.get('variant_map', {})
            handle = item.get('handle')

            if not remaining_skus or not variant_map:
                print(f"   ⚠️ Mangler data for partial produkt")
                skipped[pid]['status'] = 'done'
                continue

            # Filtrer: fjern dem der nu er i Shopify (oprettet i går)
            still_remaining = [s for s in remaining_skus if s not in shopify_skus and s in feed_by_sku]

            if not still_remaining:
                print(f"   ✅ Alle varianter allerede oprettet!")
                skipped[pid]['status'] = 'done'
                continue

            # Hvis handle ikke er sat, find det via oprettede SKUs
            if not handle:
                for created_sku in item.get('created_skus', []):
                    if created_sku in sku_to_handle:
                        handle = sku_to_handle[created_sku]
                        skipped[pid]['handle'] = handle
                        break

            if not handle:
                print(f"   ⚠️ Kan ikke finde handle for partial produkt")
                continue

            print(f"   Handle: {handle}, {len(still_remaining)} resterende varianter")

            if len(still_remaining) <= remaining_budget:
                take_skus = still_remaining
                skipped[pid]['status'] = 'done'
                skipped[pid]['created_skus'].extend(take_skus)
                print(f"   ✅ Merger alle {len(take_skus)} resterende varianter")
            else:
                take_skus = still_remaining[:remaining_budget]
                new_remaining = still_remaining[remaining_budget:]
                skipped[pid]['created_skus'].extend(take_skus)
                skipped[pid]['remaining_skus'] = new_remaining
                print(f"   ⚡ Partial merge: {len(take_skus)} nu, {len(new_remaining)} venter")

            take_variant_map = {s: variant_map[s] for s in take_skus if s in variant_map}

            product_groups.append({
                'feed_rows': feed[feed['SKU'].isin(take_skus)],
                'variant_map': take_variant_map,
                'options': item.get('options', {}),
                'existing_handle': handle,
                'is_merge': True
            })

            total_variants += len(take_skus)

        products_processed += 1

    # 5. Byg output
    merges = sum(1 for g in product_groups if g['is_merge'])
    news = len(product_groups) - merges
    print(f"\n✅ {products_processed} produkter processeret ({news} nye, {merges} merge), {total_variants} varianter")

    print(f"\n📝 Genererer XLSX filer...")
    df_new = build_new_products(product_groups, config, underkat, rum_dict, all_handles, feed)
    save_xlsx(df_new, 'output/matrixify_create_large_new.xlsx')
    print(f"   ✅ Nye: {len(df_new)} rækker")

    df_merge = build_merge_variants(product_groups, config, underkat, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, feed)
    save_xlsx(df_merge, 'output/matrixify_create_large_merge.xlsx')
    print(f"   ✅ Merge: {len(df_merge)} rækker")

    # 6. Opdater variant count (addér til eksisterende)
    new_total = daily_used + total_variants
    with open(COUNT_PATH, 'w') as f:
        f.write(str(new_total))
    print(f"   💾 Variant count opdateret: {daily_used} + {total_variants} = {new_total}")

    # 7. Gem opdateret skipped liste
    # Fjern done produkter
    skipped = {k: v for k, v in skipped.items() if v.get('status') != 'done'}
    with open(SKIPPED_PATH, 'w', encoding='utf-8') as f:
        json.dump(skipped, f, ensure_ascii=False, indent=2)
    print(f"   💾 Skipped liste: {len(skipped)} produkter tilbage")

    print(f"\n✅ SUCCESS!")
    print(f"📊 Nye: {news} grupper, {len(df_new)} rækker")
    print(f"📊 Merge: {merges} grupper, {len(df_merge)} rækker")
    print(f"📊 Varianter: {total_variants}")
    print(f"📊 Resterende i skipped: {len(skipped)}")

    gh = os.environ.get('GITHUB_OUTPUT', '')
    if gh:
        with open(gh, 'a') as f:
            f.write(f"large_variant_count={total_variants}\n")
            f.write(f"large_new_rows={len(df_new)}\n")
            f.write(f"large_merge_rows={len(df_merge)}\n")
            f.write(f"large_products={products_processed}\n")
            f.write(f"remaining_skipped={len(skipped)}\n")

except Exception as e:
    print(f"\n❌ FATAL: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
