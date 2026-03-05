import pandas as pd
import requests
import zipfile
import io
import json
import os
import sys
import re
import time
from datetime import datetime
from collections import defaultdict
from bs4 import BeautifulSoup

print("VidaXL Product Creator - Automatisk (GitHub Actions)")
print("=" * 60)

# ============================================================
# KONFIGURATION
# ============================================================
FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
MAX_GROUPS = int(os.environ.get('MAX_PRODUCTS_PER_RUN', '999'))
MAX_VARIANTS = int(os.environ.get('MAX_VARIANTS_PER_RUN', '999'))
MIN_STOCK_PRIMARY = 20
MIN_STOCK_VARIANT = 4
PRODUCT_ORDER = os.environ.get('PRODUCT_ORDER', 'newest')  # 'newest' eller 'random'
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'Kategori_Config.xlsx')

BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'da-DK,da;q=0.9,en;q=0.8',
}

missing = []
if not FEED_URL: missing.append('FEED_URL')
if not SHOPIFY_STORE: missing.append('SHOPIFY_STORE')
if not SHOPIFY_ACCESS_TOKEN: missing.append('SHOPIFY_ACCESS_TOKEN')
if missing:
    print(f"❌ Manglende: {', '.join(missing)}")
    sys.exit(1)

print(f"⚙️ Max grupper: {MAX_GROUPS}, Max varianter: {MAX_VARIANTS}, Rækkefølge: {PRODUCT_ORDER}")

# ============================================================
# HJÆLPEFUNKTIONER
# ============================================================

def normalize_sku(sku):
    if pd.isna(sku): return ''
    return str(sku).strip().replace('.0', '')

def clean_text(text):
    if pd.isna(text): return ''
    text = str(text)
    for c in ['*',':','/','\\','?','[',']','\n','\r','\t','"',"'",'<','>','|']:
        text = text.replace(c, ' ')
    return ' '.join(text.split())[:30000]

def clean_vidaxl(text):
    if pd.isna(text): return ''
    text = str(text)
    for v in ['vidaXL ', 'vidaxl ', 'VidaXL ', 'VIDAXL ', 'fra vidaXL', 'vidaXL', 'vidaxl']:
        text = text.replace(v, '')
    return text.strip()

def convert_danish_chars(text):
    if pd.isna(text): return ''
    text = str(text)
    for d, e in {'æ':'ae','Æ':'ae','ø':'oe','Ø':'oe','å':'aa','Å':'aa','ä':'ae','ö':'oe','ü':'ue'}.items():
        text = text.replace(d, e)
    return text

def title_case_danish(text):
    if pd.isna(text) or not text: return ''
    return ' '.join(w[0].upper() + w[1:].lower() if w else w for w in text.split())

def generate_handle(title, existing_handles):
    if pd.isna(title): return ''
    handle = convert_danish_chars(title.lower())
    handle = re.sub(r'[^a-z0-9\s-]', '', handle)
    handle = re.sub(r'\s+', '-', handle)
    handle = re.sub(r'-+', '-', handle).strip('-')
    if len(handle) > 255: handle = handle[:255].rsplit('-', 1)[0]
    base = handle
    counter = 2
    while handle in existing_handles:
        suffix = f"-{counter}"
        handle = base[:255-len(suffix)] + suffix if len(base)+len(suffix) > 255 else f"{base}{suffix}"
        counter += 1
    existing_handles.add(handle)
    return handle

def calculate_price(base_price, slutciffer=9):
    rounded = round(base_price)
    last = rounded % 10
    if last == 0: return rounded - 1
    elif last == 9: return rounded
    else: return (int(rounded / 10) + 1) * 10 - 1

def validate_url(url):
    if pd.isna(url) or not url: return False
    return str(url).strip().startswith(('http://', 'https://'))

def generate_seo_description(html_text, max_length=160):
    if pd.isna(html_text): return ''
    text = re.sub('<.*?>', '', str(html_text))
    text = ' '.join(text.split())
    if len(text) <= max_length: return text
    t = text[:max_length]
    lp = t.rfind('.')
    if lp > 0: return text[:lp + 1]
    ls = t.rfind(' ')
    return text[:ls] + '...' if ls > 0 else t + '...'

def extract_tags(category):
    if pd.isna(category): return []
    parts = [p.strip() for p in str(category).split(' > ')]
    tags = list(parts)
    if len(parts) > 1: tags.append(' > '.join(parts))
    return tags

def get_all_images(row):
    images = []
    for i in range(1, 22):
        if i <= 12: col = f'Image {i}'
        elif i == 13: col = 'image 13'
        elif i == 14: col = 'Image 14'
        else: col = f'image {i}'
        if col in row.index and pd.notna(row[col]):
            img = str(row[col]).strip()
            if validate_url(img): images.append(img)
    return images

# ============================================================
# TITEL RENSNING
# ============================================================

STICKY_AFTER_NUMBER = {'stk.', 'stk', 'sæt', 'dele', 'pak', 'pakke', 'par'}

def clean_title_from_options(title, option_values):
    if pd.isna(title) or not title: return ''
    title = clean_vidaxl(title)

    for opt_val in option_values:
        if not opt_val: continue
        opt_str = str(opt_val).strip()
        pattern = re.compile(re.escape(opt_str), re.IGNORECASE)
        title = pattern.sub(' ', title)

        if opt_str.isdigit():
            words = title.split()
            cleaned = []
            for w in words:
                if w.lower().rstrip('.,') in STICKY_AFTER_NUMBER:
                    continue
                cleaned.append(w)
            title = ' '.join(cleaned)

    title = re.sub(r'(?<!\d)\s+[Cc][Mm]\.?\b', '', title)
    title = re.sub(r'\b[xX]\b', '', title)
    title = re.sub(r'(?<!\d)\s+[Mm][Mm]\.?\b', '', title)
    title = re.sub(r'\s+', ' ', title)
    title = title.strip(' ,-–')
    return title

# ============================================================
# DATA HENTNING
# ============================================================

def fetch_feed(url):
    print(f"\n📥 Henter feed...")
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    print(f"   {len(resp.content)/1024/1024:.1f} MB")
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csvs = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csvs: raise Exception("Ingen CSV i ZIP")
        with zf.open(csvs[0]) as f:
            df = pd.read_csv(f, encoding='utf-8', on_bad_lines='skip')
    return df

def fetch_shopify_sku_handle_map(store, token):
    print(f"\n📥 Henter Shopify SKU→handle map via GraphQL...")
    sku_to_handle = {}
    all_handles = set()
    url = f"https://{store}/admin/api/2024-10/graphql.json"
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    has_next, cursor, total = True, None, 0

    while has_next:
        after = f', after: "{cursor}"' if cursor else ''
        q = '''
        {
            productVariants(first: 250%s) {
                edges {
                    node {
                        sku
                        product {
                            handle
                        }
                    }
                    cursor
                }
                pageInfo { hasNextPage }
            }
        }
        ''' % after

        resp = requests.post(url, headers=headers, json={'query': q}, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if 'errors' in data:
            if any('Throttled' in str(e) for e in data['errors']):
                time.sleep(2); continue
            raise Exception(f"GraphQL: {data['errors']}")

        ext = data.get('extensions',{}).get('cost',{}).get('throttleStatus',{})
        if ext.get('currentlyAvailable', 1000) < 100: time.sleep(1)

        edges = data.get('data',{}).get('productVariants',{}).get('edges',[])
        for e in edges:
            node = e.get('node',{})
            sku = node.get('sku')
            handle = node.get('product',{}).get('handle','')
            if sku:
                norm = normalize_sku(sku)
                sku_to_handle[norm] = handle
            if handle:
                all_handles.add(handle)

        total += len(edges)
        pi = data.get('data',{}).get('productVariants',{}).get('pageInfo',{})
        has_next = pi.get('hasNextPage', False)
        if has_next and edges: cursor = edges[-1].get('cursor')
        if total % 5000 == 0: print(f"   {total:,} varianter...")

    print(f"✅ {len(sku_to_handle):,} SKU→handle, {len(all_handles):,} handles")
    return sku_to_handle, all_handles

# ============================================================
# VIDAXL SCRAPER
# ============================================================

def scrape_vidaxl(url):
    result = {'master_pid': None, 'options': {}, 'success': False}
    try:
        resp = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
        if resp.status_code != 200: return result
        html = resp.text
        soup = BeautifulSoup(html, 'html.parser')

        pid_match = re.search(r'pid=([A-Z]\d+)', html)
        if pid_match:
            result['master_pid'] = pid_match.group(1)
        if not result['master_pid']:
            m = re.search(r'dwvar_([A-Z]\d+)_', html)
            if m: result['master_pid'] = m.group(1)

        if not result['master_pid']:
            result['success'] = True
            return result

        # FARVE fra <select>
        color_select = soup.find('select', {'name': 'color-attribute__value'})
        if color_select:
            colors = []
            for opt in color_select.find_all('option'):
                val = opt.get('value', '')
                if not val: continue
                display = opt.get_text(strip=True)
                colors.append({'value': val, 'display': display})
            if colors:
                result['options']['color'] = {'display_name': 'Farve', 'values': colors}

        # ANDRE options fra alle elementer med data-action-url
        all_action_elems = soup.find_all(attrs={'data-action-url': re.compile('Product-Variation')})
        other_options = {}

        for elem in all_action_elems:
            action_url = elem.get('data-action-url', '')
            attr_value = elem.get('data-attr-value', '')
            display_value = (
                elem.get('data-display-value', '') or
                elem.get('aria-label', '') or
                elem.get_text(strip=True) or
                attr_value.replace('_', ' ')
            )

            dwvar_matches = re.findall(r'dwvar_[^_]+_(\w+)=([^&]*)', action_url)
            for attr_name, url_value in dwvar_matches:
                if attr_name == 'color': continue
                if attr_name not in other_options:
                    other_options[attr_name] = []
                if attr_value and not any(e['value'] == attr_value for e in other_options[attr_name]):
                    other_options[attr_name].append({'value': attr_value, 'display': display_value.strip()})

        for attr_name, values in other_options.items():
            display_name = attr_name
            label_div = soup.find('div', class_=re.compile(f'{attr_name}.*font-weight-bold'))
            if not label_div:
                label_div = soup.find('div', attrs={'for': re.compile(f'{attr_name}')}, class_=re.compile('font-weight-bold'))
            if label_div:
                lt = re.sub(r'\(\d+\s*tilgængelige\s*muligheder\)', '', label_div.get_text(strip=True)).strip()
                if lt: display_name = lt
            result['options'][attr_name] = {'display_name': display_name, 'values': values}

        result['success'] = True
    except Exception as e:
        print(f"   ⚠️ Scrape fejl: {e}")
    return result


def fetch_variant_skus(master_pid, options):
    base_url = "https://www.vidaxl.dk/on/demandware.store/Sites-vidaxl-dk-Site/da_DK/Product-Variation"
    option_names = list(options.keys())
    option_values_list = [options[name]['values'] for name in option_names]

    if not option_values_list: return {}

    combos = [{}]
    for name, values in zip(option_names, option_values_list):
        new = []
        for combo in combos:
            for val in values:
                c = dict(combo)
                c[name] = val
                new.append(c)
        combos = new

    print(f"   Henter SKUs for {len(combos)} kombinationer...")
    variant_map = {}

    for i, combo in enumerate(combos):
        params = {f'dwvar_{master_pid}_{name}': val['value'] for name, val in combo.items()}
        params['pid'] = master_pid
        params['quantity'] = '1'

        try:
            resp = requests.get(base_url, params=params, headers={
                **BROWSER_HEADERS,
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'X-Requested-With': 'XMLHttpRequest'
            }, timeout=15)

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    sku = data.get('product', {}).get('SKU', '')
                    if sku:
                        opt_displays = {}
                        for name, val in combo.items():
                            opt_displays[options[name]['display_name']] = val['display']
                        variant_map[normalize_sku(sku)] = opt_displays
                except json.JSONDecodeError:
                    pass

            if (i + 1) % 10 == 0:
                time.sleep(1)
            else:
                time.sleep(0.3)
        except Exception as e:
            print(f"   ⚠️ API fejl kombination {i+1}: {e}")
            time.sleep(1)

    print(f"   ✅ {len(variant_map)} varianter med SKU")
    return variant_map

# ============================================================
# MATRIXIFY OUTPUT
# ============================================================

def build_matrixify(product_groups, config, underkat_config, rum_dict, existing_handles):
    rows = []
    handles_used = existing_handles.copy()

    for group in product_groups:
        feed_rows = group['feed_rows']
        variant_map = group['variant_map']
        option_struct = group['options']
        existing_handle = group.get('existing_handle', None)
        is_merge = group.get('is_merge', False)

        if len(feed_rows) == 0: continue

        first = feed_rows.iloc[0]
        hovedkat = str(first['Category']).split(' > ')[0] if pd.notna(first['Category']) else ''

        cat_cfg = config[config['Kategori_Config'] == hovedkat]
        markup = float(cat_cfg['Markup %'].iloc[0]) if len(cat_cfg) > 0 and pd.notna(cat_cfg['Markup %'].iloc[0]) else 70.0
        slutciffer = int(cat_cfg['Slutciffer'].iloc[0]) if len(cat_cfg) > 0 and pd.notna(cat_cfg['Slutciffer'].iloc[0]) else 9
        compare_pct = float(cat_cfg['Sammenligningspris %'].iloc[0]) if len(cat_cfg) > 0 and pd.notna(cat_cfg['Sammenligningspris %'].iloc[0]) else 0

        if not underkat_config.empty:
            cs = str(first['Category']).strip() if pd.notna(first['Category']) else ''
            ukat = underkat_config[underkat_config['Underkategori_Config'].astype(str).str.strip() == cs]
            if len(ukat) > 0:
                if pd.notna(ukat['Markup %'].iloc[0]): markup = float(ukat['Markup %'].iloc[0])
                if 'Sammenligningspris %' in ukat.columns and pd.notna(ukat['Sammenligningspris %'].iloc[0]):
                    compare_pct = float(ukat['Sammenligningspris %'].iloc[0])

        # Titel
        all_opt_displays = set()
        for od in option_struct.values():
            for v in od.get('values', []):
                all_opt_displays.add(v['display'])

        raw_title = str(first['Title']) if pd.notna(first['Title']) else ''
        clean_t = clean_title_from_options(raw_title, list(all_opt_displays))
        final_title = title_case_danish(clean_t)
        if not final_title or len(final_title) < 5:
            final_title = title_case_danish(clean_vidaxl(raw_title))

        # Handle
        if existing_handle:
            handle = existing_handle
        else:
            handle = generate_handle(final_title, handles_used)

        # Irrelevante options
        if len(variant_map) > 1:
            all_ov = defaultdict(set)
            for opts in variant_map.values():
                for k, v in opts.items(): all_ov[k].add(v)
            irrelevant = {k for k, v in all_ov.items() if len(v) <= 1}
        else:
            irrelevant = set()

        is_first_new = True
        variant_pos = 0

        for _, row in feed_rows.iterrows():
            try:
                sku = normalize_sku(row['SKU'])
                cost_kr = float(row['B2B price'])
                price = calculate_price(cost_kr * (1 + markup / 100), slutciffer)
                c_price = ''
                if compare_pct > 0:
                    c_price = calculate_price(price / (1 - compare_pct / 100), slutciffer)

                tags_list = []
                if pd.notna(row['Category']): tags_list.extend(extract_tags(row['Category']))
                if pd.notna(row.get('Brand')): tags_list.append(str(row['Brand']))
                if pd.notna(row.get('Color')): tags_list.append(str(row['Color']))
                if 'Parcel_or_pallet' in row.index and pd.notna(row['Parcel_or_pallet']):
                    pv = str(row['Parcel_or_pallet']).strip().lower()
                    if pv == 'parcel': tags_list.append('Parcel')
                    elif pv == 'pallet': tags_list.append('Pallet')
                if rum_dict and pd.notna(row['Category']):
                    cs = str(row['Category']).strip()
                    if cs in rum_dict and pd.notna(rum_dict[cs]):
                        tags_list.append(str(rum_dict[cs]))
                seen = set()
                tags = ','.join(t for t in tags_list if not (t in seen or seen.add(t)))

                clean_html = clean_vidaxl(row.get('HTML_description', ''))
                product_type = row['Category'].split(' > ')[-1].strip() if pd.notna(row['Category']) else ''
                seo_title = final_title[:70] if len(final_title) <= 70 else final_title[:67] + '...'
                seo_desc = generate_seo_description(clean_html)
                all_images = get_all_images(row)

                weight = 0
                if pd.notna(row.get('Weight')):
                    try: weight = int(float(str(row['Weight']).replace(',', '.')) * 1000)
                    except: pass

                variant_pos += 1
                opts = variant_map.get(sku, {})
                relevant = {k: v for k, v in opts.items() if k not in irrelevant}
                opt_list = list(relevant.items())

                write_product_fields = is_first_new and not is_merge

                # Variant Position: kun for nye produkter, tom for merge (Matrixify tilføjer i enden)
                v_position = variant_pos if not is_merge else ''

                product_row = {
                    'Command': 'MERGE',
                    'Handle': handle,
                    'Title': final_title if write_product_fields else '',
                    'Body HTML': clean_html if write_product_fields else '',
                    'Vendor': row.get('Brand', '') if write_product_fields else '',
                    'Type': product_type if write_product_fields else '',
                    'Tags': tags if write_product_fields else '',
                    'Published': 'TRUE' if write_product_fields else '',
                    'Status': 'active' if write_product_fields else '',
                    'Published Scope': 'global' if write_product_fields else '',
                    'Variant SKU': sku,
                    'Variant Barcode': str(row.get('EAN', '')),
                    'Variant Position': v_position,
                    'Variant Price': int(price),
                    'Variant Compare At Price': int(c_price) if c_price else '',
                    'Variant Cost': int(cost_kr),
                    'Variant Weight': weight,
                    'Variant Weight Unit': 'g',
                    'Variant Inventory Tracker': 'shopify',
                    'Variant Inventory Policy': 'deny',
                    'Variant Inventory Qty': int(row.get('Stock', 0) or 0),
                    'Variant Fulfillment Service': 'manual',
                    'Variant Requires Shipping': 'TRUE',
                    'Variant Taxable': 'TRUE',
                    'SEO Title': seo_title if write_product_fields else '',
                    'SEO Description': seo_desc if write_product_fields else '',
                    'Google Shopping / MPN': sku,
                    'Google Shopping / Condition': 'new',
                    'Variant Image': all_images[0] if all_images else '',
                    'Image Src': '',
                    'Image Position': '',
                    'Image Alt Text': '',
                }

                for i in range(1, 4):
                    if i <= len(opt_list):
                        product_row[f'Option{i} Name'] = opt_list[i-1][0]
                        product_row[f'Option{i} Value'] = opt_list[i-1][1]
                    else:
                        product_row[f'Option{i} Name'] = ''
                        product_row[f'Option{i} Value'] = ''

                # SKU metafield - altid udfyldt for alle varianter
                product_row['Variant Metafield: custom.sku [single_line_text_field]'] = sku

                if not (is_first_new and not is_merge):
                    product_row['Variant Metafield: custom.produktinfo [multi_line_text_field]'] = clean_html
                    if all_images:
                        product_row['Variant Metafield: custom.variantbilleder [list.single_line_text_field]'] = ', '.join(all_images)

                if write_product_fields and all_images:
                    product_row['Image Src'] = all_images[0]
                    product_row['Image Position'] = '1'
                    product_row['Image Alt Text'] = f"{final_title} - Hovedbillede"
                    rows.append(product_row)
                    for img_i, img_url in enumerate(all_images[1:], 2):
                        img_row = {col: '' for col in product_row.keys()}
                        img_row['Handle'] = handle
                        img_row['Command'] = 'MERGE'
                        img_row['Image Src'] = img_url
                        img_row['Image Position'] = str(img_i)
                        img_row['Image Alt Text'] = f"{final_title} - Billede {img_i}"
                        rows.append(img_row)
                else:
                    rows.append(product_row)

                is_first_new = False
            except Exception as e:
                print(f"   ⚠️ Fejl SKU {row.get('SKU','?')}: {str(e)[:100]}")
                continue

    return pd.DataFrame(rows) if rows else pd.DataFrame()

# ============================================================
# HOVEDPROCESSERING
# ============================================================

try:
    # 1. Hent data
    feed = fetch_feed(FEED_URL)
    feed['SKU'] = feed['SKU'].apply(normalize_sku)
    feed['Stock'] = pd.to_numeric(feed['Stock'], errors='coerce').fillna(0)
    feed['B2B price'] = pd.to_numeric(feed['B2B price'], errors='coerce').fillna(0)
    print(f"✅ {len(feed):,} produkter i feed")

    sku_to_handle, all_handles = fetch_shopify_sku_handle_map(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN)
    shopify_skus = set(sku_to_handle.keys())

    feed_by_sku = {}
    for _, r in feed.iterrows():
        s = normalize_sku(r['SKU'])
        if s and s not in feed_by_sku:
            feed_by_sku[s] = r

    # 2. Config
    print(f"\n📋 Læser config...")
    config = pd.read_excel(CONFIG_PATH, sheet_name='Kategori_Config')
    config['Markup %'] = pd.to_numeric(config['Markup %'], errors='coerce')
    config['Slutciffer'] = pd.to_numeric(config['Slutciffer'], errors='coerce')
    config['Sammenligningspris %'] = pd.to_numeric(config['Sammenligningspris %'], errors='coerce')

    try:
        underkat = pd.read_excel(CONFIG_PATH, sheet_name='Underkategori_Config')
        if 'Markup %' in underkat.columns: underkat['Markup %'] = pd.to_numeric(underkat['Markup %'], errors='coerce')
        if 'Sammenligningspris %' in underkat.columns: underkat['Sammenligningspris %'] = pd.to_numeric(underkat['Sammenligningspris %'], errors='coerce')
    except: underkat = pd.DataFrame()

    try:
        rum_map = pd.read_excel(CONFIG_PATH, sheet_name='Rum_Mapping')
        rum_dict = dict(zip(rum_map.iloc[:, 0], rum_map.iloc[:, 1]))
    except: rum_dict = {}

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

    # Sortér efter valgt rækkefølge
    if PRODUCT_ORDER == 'random':
        candidates = candidates.sample(frac=1, random_state=int(time.time()) % 10000).reset_index(drop=True)
        print(f"   Rækkefølge: TILFÆLDIG")
    else:
        candidates['SKU_num'] = pd.to_numeric(candidates['SKU'], errors='coerce')
        candidates = candidates.sort_values('SKU_num', ascending=False).reset_index(drop=True)
        print(f"   Rækkefølge: NYESTE FØRST")

    print(f"✅ {len(candidates):,} kandidater")

    if len(candidates) == 0:
        print("\n⚠️ INGEN NYE PRODUKTER!")
        pd.DataFrame().to_excel('output/matrixify_create.xlsx', index=False, engine='openpyxl')
        sys.exit(0)

    # 4. Scrape og grupper
    print(f"\n🔍 Scraper VidaXL...")
    product_groups = []
    processed_skus = set()
    total_variants = 0
    scrape_count = 0

    for _, row in candidates.iterrows():
        sku = normalize_sku(row['SKU'])
        if sku in processed_skus: continue

        if len(product_groups) >= MAX_GROUPS:
            print(f"   Max {MAX_GROUPS} grupper nået"); break
        if total_variants >= MAX_VARIANTS:
            print(f"   Max {MAX_VARIANTS} varianter nået"); break

        url = row.get('Link', '')
        if not validate_url(url):
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}},
                'options': {},
                'existing_handle': None,
                'is_merge': False
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
                'variant_map': {sku: {}},
                'options': {},
                'existing_handle': None,
                'is_merge': False
            })
            total_variants += 1
            print(f"   → Single produkt")
            continue

        print(f"   PID: {scrape['master_pid']}")
        for on, od in scrape['options'].items():
            print(f"   {od['display_name']}: {len(od['values'])} værdier")

        variant_map = fetch_variant_skus(scrape['master_pid'], scrape['options'])

        if not variant_map:
            processed_skus.add(sku)
            product_groups.append({
                'feed_rows': feed[feed['SKU'] == sku],
                'variant_map': {sku: {}},
                'options': {},
                'existing_handle': None,
                'is_merge': False
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
            elif v_sku in processed_skus:
                continue
            elif v_sku not in feed_by_sku:
                continue
            else:
                fr = feed_by_sku[v_sku]
                stock = float(fr.get('Stock', 0) or 0)
                price = float(fr.get('B2B price', 0) or 0)
                if stock >= MIN_STOCK_VARIANT and price > 0:
                    new_skus.append(v_sku)

        if not new_skus:
            print(f"   → Ingen nye gyldige varianter")
            processed_skus.add(sku)
            continue

        if total_variants + len(new_skus) > MAX_VARIANTS:
            print(f"   → Overskriver variant-cap ({total_variants}+{len(new_skus)}>{MAX_VARIANTS})")
            break

        is_merge = existing_handle_for_group is not None

        if is_merge:
            print(f"   → MERGE til: {existing_handle_for_group} ({len(existing_skus_in_group)} eksist., {len(new_skus)} nye)")
        else:
            print(f"   → NYT produkt med {len(new_skus)} varianter")

        group_feed = feed[feed['SKU'].isin(new_skus)].copy()
        new_variant_map = {s: variant_map[s] for s in new_skus if s in variant_map}

        for s in new_skus: processed_skus.add(s)

        product_groups.append({
            'feed_rows': group_feed,
            'variant_map': new_variant_map,
            'options': scrape['options'],
            'existing_handle': existing_handle_for_group,
            'is_merge': is_merge
        })

        total_variants += len(new_skus)
        print(f"   → {len(new_skus)} varianter (total: {total_variants})")

    print(f"\n✅ {scrape_count} sider, {len(product_groups)} grupper, {total_variants} varianter")

    if not product_groups:
        print("\n⚠️ Ingen grupper!")
        pd.DataFrame().to_excel('output/matrixify_create.xlsx', index=False, engine='openpyxl')
        sys.exit(0)

    # 5. Byg output
    print(f"\n📝 Genererer XLSX...")
    matrixify = build_matrixify(product_groups, config, underkat, rum_dict, all_handles)

    if len(matrixify) == 0:
        print("⚠️ Tom output!")
        pd.DataFrame().to_excel('output/matrixify_create.xlsx', index=False, engine='openpyxl')
        sys.exit(0)

    # 6. Gem som XLSX
    matrixify.to_excel('output/matrixify_create.xlsx', index=False, engine='openpyxl')

    merges = sum(1 for g in product_groups if g['is_merge'])
    news = len(product_groups) - merges

    print(f"\n✅ SUCCESS!")
    print(f"📊 {len(matrixify):,} rækker")
    print(f"   Nye produkter: {news}")
    print(f"   Merge: {merges}")
    print(f"   Varianter: {total_variants}")

    gh = os.environ.get('GITHUB_OUTPUT', '')
    if gh:
        with open(gh, 'a') as f:
            f.write(f"product_count={len(product_groups)}\n")
            f.write(f"variant_count={total_variants}\n")
            f.write(f"row_count={len(matrixify)}\n")
            f.write(f"merge_count={merges}\n")
            f.write(f"new_count={news}\n")

except Exception as e:
    print(f"\n❌ FATAL: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
