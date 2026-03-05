"""
VidaXL Product Creator v3.0 — HURTIG daglig kørsel
Opretter produkter med < 100 kombinationer.
Gemmer 100+ combo produkter til skipped_large_products.json.
Gemmer variant-count til daily_variant_count.txt.
"""
import pandas as pd
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from product_utils import *

print("VidaXL Product Creator v3.0 - HURTIG daglig kørsel")
print("=" * 60)

# ============================================================
# KONFIGURATION
# ============================================================
FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
MAX_GROUPS = int(os.environ.get('MAX_PRODUCTS_PER_RUN', '999'))
MAX_VARIANTS_SOFT = int(os.environ.get('MAX_VARIANTS_PER_RUN', '999'))
MAX_VARIANTS_HARD = 999
MAX_COMBOS = 100  # Produkter med flere kombinationer end dette skippes
MIN_STOCK_PRIMARY = 20
MIN_STOCK_VARIANT = 4
PRODUCT_ORDER = os.environ.get('PRODUCT_ORDER', 'newest')
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

print(f"⚙️ Max grupper: {MAX_GROUPS}, Blød grænse: {MAX_VARIANTS_SOFT}, Hård: {MAX_VARIANTS_HARD}, Rækkefølge: {PRODUCT_ORDER}")
print(f"⚙️ Max kombinationer per produkt: {MAX_COMBOS}")

try:
    # 1. Hent data
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

    # 2. Config
    print(f"\n📋 Læser config...")
    config, underkat, rum_dict = load_config(CONFIG_PATH)
    aktive = config[config['Import?'] == 'JA']['Kategori_Config'].tolist()
    print(f"✅ Aktive kategorier: {', '.join(aktive)}")

    # 3. Kandidater
    print(f"\n🔍 Filtrerer kandidater...")
    candidates = feed[
        (~feed['SKU'].isin(shopify_skus)) &
        (feed['Stock'] >= MIN_STOCK_PRIMARY) &
        (feed['B2B price'] > 0)
    ].copy()
    candidates['Hovedkategori'] = candidates['Category'].str.split(' > ').str[0]
    candidates = candidates[candidates['Hovedkategori'].isin(aktive)].copy()

    if PRODUCT_ORDER == 'random':
        candidates = candidates.sample(frac=1, random_state=int(time.time()) % 10000).reset_index(drop=True)
        print(f"   Rækkefølge: TILFÆLDIG")
    else:
        candidates['SKU_num'] = pd.to_numeric(candidates['SKU'], errors='coerce')
        candidates = candidates.sort_values('SKU_num', ascending=False).reset_index(drop=True)
        print(f"   Rækkefølge: NYESTE FØRST")

    print(f"✅ {len(candidates):,} kandidater")

    # Load eksisterende skipped liste (behold partial produkter)
    skipped = {}
    if os.path.exists(SKIPPED_PATH):
        try:
            with open(SKIPPED_PATH, 'r', encoding='utf-8') as f:
                skipped = json.load(f)
            # Fjern done produkter
            skipped = {k: v for k, v in skipped.items() if v.get('status') != 'done'}
            print(f"📋 {len(skipped)} eksisterende store produkter i skipped-listen")
        except: skipped = {}

    if len(candidates) == 0:
        print("\n⚠️ INGEN NYE PRODUKTER!")
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_new.xlsx')
        save_xlsx(pd.DataFrame(), 'output/matrixify_create_merge.xlsx')
        with open(COUNT_PATH, 'w') as f: f.write('0')
        sys.exit(0)

    # 4. Scrape og grupper
    print(f"\n🔍 Scraper VidaXL (max {MAX_COMBOS} kombinationer)...")
    product_groups = []
    processed_skus = set()
    total_variants = 0
    scrape_count = 0
    skipped_count = 0

    for _, row in candidates.iterrows():
        sku = normalize_sku(row['SKU'])
        if sku in processed_skus: continue

        if len(product_groups) >= MAX_GROUPS:
            print(f"   Max {MAX_GROUPS} grupper nået"); break
        if total_variants >= MAX_VARIANTS_SOFT:
            print(f"   Blød grænse nået ({total_variants} ≥ {MAX_VARIANTS_SOFT})"); break

        url = row.get('Link', '')
        if not validate_url(url):
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}}, 'options': {},
                'existing_handle': None, 'is_merge': False
            })
            total_variants += 1
            continue

        print(f"\n📦 [{len(product_groups)+1}] SKU {sku}...")
        scrape = scrape_vidaxl(url)
        scrape_count += 1
        time.sleep(1)

        if not scrape['success'] or not scrape['master_pid'] or not scrape['options']:
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}}, 'options': {},
                'existing_handle': None, 'is_merge': False
            })
            total_variants += 1
            print(f"   → Single produkt")
            continue

        print(f"   PID: {scrape['master_pid']}")
        for on, od in scrape['options'].items():
            print(f"   {od['display_name']}: {len(od['values'])} værdier")

        # Check antal kombinationer
        num_combos = count_combinations(scrape['options'])
        if num_combos >= MAX_COMBOS:
            print(f"   ⏭️ SKIPPED: {num_combos} kombinationer → gemmes til stor kørsel")
            skipped_count += 1

            # Gem til skipped-listen (kun hvis ikke allerede der)
            pid = scrape['master_pid']
            if pid not in skipped:
                skipped[pid] = {
                    'status': 'pending',
                    'handle': None,
                    'url': url,
                    'title': str(row.get('Title', '')),
                    'master_pid': pid,
                    'sku': sku,
                    'num_combos': num_combos,
                    'options': scrape['options'],
                    'variant_map': None,
                    'created_skus': [],
                    'remaining_skus': None,
                }

            processed_skus.add(sku)
            continue

        # Normal scraping for små produkter
        variant_map = fetch_variant_skus(scrape['master_pid'], scrape['options'])

        if not variant_map:
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}}, 'options': {},
                'existing_handle': None, 'is_merge': False
            })
            total_variants += 1
            continue

        # Kategoriser varianter
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
            print(f"   → Ingen nye varianter")
            processed_skus.add(sku)
            continue

        if total_variants + len(new_skus) > MAX_VARIANTS_HARD:
            print(f"   → Over hård grænse ({total_variants}+{len(new_skus)}>{MAX_VARIANTS_HARD}), springer over")
            processed_skus.add(sku)
            continue

        is_merge = existing_handle_for_group is not None
        if is_merge:
            print(f"   → MERGE til: {existing_handle_for_group} ({len(existing_skus_in_group)} eksist., {len(new_skus)} nye)")
        else:
            print(f"   → NYT produkt med {len(new_skus)} varianter")

        group_feed = feed[feed['SKU'].isin(new_skus)].copy()
        new_variant_map = {s: variant_map[s] for s in new_skus if s in variant_map}
        for s in new_skus: processed_skus.add(s)

        product_groups.append({
            'feed_rows': group_feed, 'variant_map': new_variant_map,
            'options': scrape['options'],
            'existing_handle': existing_handle_for_group, 'is_merge': is_merge,
            'existing_skus': existing_skus_in_group if is_merge else [],
            'all_variant_map': variant_map if is_merge else {}
        })
        total_variants += len(new_skus)
        print(f"   → {len(new_skus)} varianter (total: {total_variants})")

    merges = sum(1 for g in product_groups if g['is_merge'])
    news = len(product_groups) - merges
    print(f"\n✅ {scrape_count} sider, {len(product_groups)} grupper ({news} nye, {merges} merge), {total_variants} varianter")
    print(f"   ⏭️ {skipped_count} produkter skipped (100+ kombos)")

    # 5. Byg output
    print(f"\n📝 Genererer XLSX filer...")
    df_new = build_new_products(product_groups, config, underkat, rum_dict, all_handles, feed)
    save_xlsx(df_new, 'output/matrixify_create_new.xlsx')
    print(f"   ✅ Nye: {len(df_new)} rækker")

    df_merge = build_merge_variants(product_groups, config, underkat, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, feed)
    save_xlsx(df_merge, 'output/matrixify_create_merge.xlsx')
    print(f"   ✅ Merge: {len(df_merge)} rækker")

    # 6. Gem variant count og skipped liste
    with open(COUNT_PATH, 'w') as f:
        f.write(str(total_variants))
    print(f"   💾 Variant count: {total_variants} → {COUNT_PATH}")

    with open(SKIPPED_PATH, 'w', encoding='utf-8') as f:
        json.dump(skipped, f, ensure_ascii=False, indent=2)
    print(f"   💾 Skipped liste: {len(skipped)} produkter → {SKIPPED_PATH}")

    print(f"\n✅ SUCCESS!")
    print(f"📊 Nye: {news} grupper, {len(df_new)} rækker")
    print(f"📊 Merge: {merges} grupper, {len(df_merge)} rækker")
    print(f"📊 Varianter: {total_variants}")
    print(f"📊 Skipped (store): {len(skipped)}")

    gh = os.environ.get('GITHUB_OUTPUT', '')
    if gh:
        with open(gh, 'a') as f:
            f.write(f"product_count={len(product_groups)}\n")
            f.write(f"variant_count={total_variants}\n")
            f.write(f"new_rows={len(df_new)}\n")
            f.write(f"merge_rows={len(df_merge)}\n")
            f.write(f"merge_count={merges}\n")
            f.write(f"new_count={news}\n")
            f.write(f"skipped_count={len(skipped)}\n")

except Exception as e:
    print(f"\n❌ FATAL: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
