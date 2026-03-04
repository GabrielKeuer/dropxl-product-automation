import pandas as pd
import requests
import zipfile
import io
import json
import os
import sys
import re
import time
import random
from datetime import datetime
from collections import defaultdict
from bs4 import BeautifulSoup
from urllib.parse import unquote

print("VidaXL Product Creator - Automatisk (GitHub Actions)")
print("=" * 60)

# ============================================================
# KONFIGURATION
# ============================================================
FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
MAX_PRODUCTS_PER_RUN = int(os.environ.get('MAX_PRODUCTS_PER_RUN', '50'))  # Antal produktGRUPPER per kørsel
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'Kategori_Config.xlsx')

HEADERS_BROWSER = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'da-DK,da;q=0.9,en;q=0.8',
}

# Valider environment variables
missing = []
if not FEED_URL: missing.append('FEED_URL')
if not SHOPIFY_STORE: missing.append('SHOPIFY_STORE')
if not SHOPIFY_ACCESS_TOKEN: missing.append('SHOPIFY_ACCESS_TOKEN')
if missing:
    print(f"❌ Manglende environment variables: {', '.join(missing)}")
    sys.exit(1)

# ============================================================
# HJÆLPEFUNKTIONER
# ============================================================

def normalize_sku(sku):
    if pd.isna(sku): return ''
    return str(sku).strip().replace('.0', '')

def clean_text(text):
    if pd.isna(text): return ''
    text = str(text)
    for char in ['*', ':', '/', '\\', '?', '[', ']', '\n', '\r', '\t', '"', "'", '<', '>', '|']:
        text = text.replace(char, ' ')
    return ' '.join(text.split())[:30000]

def clean_vidaxl(text):
    if pd.isna(text): return ''
    text = str(text)
    for variant in ['vidaXL ', 'vidaxl ', 'VidaXL ', 'VIDAXL ', 'fra vidaXL', 'vidaXL', 'vidaxl']:
        text = text.replace(variant, '')
    return text.strip()

def convert_danish_chars(text):
    if pd.isna(text): return ''
    text = str(text)
    for danish, english in {'æ':'ae','Æ':'ae','ø':'oe','Ø':'oe','å':'aa','Å':'aa','ä':'ae','Ä':'ae','ö':'oe','Ö':'oe','ü':'ue','Ü':'ue'}.items():
        text = text.replace(danish, english)
    return text

def title_case_danish(text):
    if pd.isna(text) or not text: return ''
    return ' '.join(w[0].upper() + w[1:].lower() if len(w) > 0 else w for w in text.split())

def normalize_for_comparison(text):
    if pd.isna(text): return ''
    return re.sub(r'[,:;!?.]', '', str(text).lower()).strip()

def find_common_title(titles):
    title_list = [clean_vidaxl(str(t)) for t in titles if pd.notna(t)]
    if not title_list: return ""
    if len(title_list) == 1: return title_list[0]
    
    all_words = []
    for title in title_list:
        word_pairs = [(w, normalize_for_comparison(w)) for w in title.split()]
        all_words.append(word_pairs)
    
    reference = all_words[0]
    common = []
    for orig, norm in reference:
        if norm in ('cm', 'x'): continue
        if all(any(norm == on for _, on in other) for other in all_words[1:]):
            common.append(orig)
    
    result = ' '.join(common)
    result = re.sub(r'(?<!\d)\s+[Cc]m\b', '', result)
    result = re.sub(r'\b[xX]\b', '', result)
    result = ' '.join(result.split())
    
    if len(result) < 15 or result.count(' ') < 1:
        result = title_list[0]
    return result

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

def calculate_price_with_slutciffer(base_price, slutciffer=9):
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
    truncated = text[:max_length]
    last_period = truncated.rfind('.')
    if last_period > 0: return text[:last_period + 1]
    last_space = truncated.rfind(' ')
    return text[:last_space] + '...' if last_space > 0 else truncated + '...'

def extract_hierarchical_tags(category):
    if pd.isna(category): return []
    parts = [p.strip() for p in str(category).split(' > ')]
    tags = list(parts)
    if len(parts) > 1: tags.append(' > '.join(parts))
    return tags

# ============================================================
# DATA HENTNING
# ============================================================

def fetch_feed_data(url):
    print(f"\n📥 Henter feed data fra URL...")
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    print(f"   Download: {len(response.content) / 1024 / 1024:.1f} MB")
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files: raise Exception("Ingen CSV fil fundet i ZIP")
        print(f"   Udpakker: {csv_files[0]}")
        with zf.open(csv_files[0]) as csv_file:
            df = pd.read_csv(csv_file, encoding='utf-8', on_bad_lines='skip')
    return df

def fetch_shopify_skus_graphql(store, token):
    print(f"\n📥 Henter Shopify SKUs via GraphQL API...")
    skus = set()
    url = f"https://{store}/admin/api/2024-10/graphql.json"
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    has_next = True
    cursor = None
    total = 0
    while has_next:
        if cursor:
            q = '{ productVariants(first: 250, after: "%s") { edges { node { sku } cursor } pageInfo { hasNextPage } } }' % cursor
        else:
            q = '{ productVariants(first: 250) { edges { node { sku } cursor } pageInfo { hasNextPage } } }'
        resp = requests.post(url, headers=headers, json={'query': q}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if 'errors' in data:
            if any('Throttled' in str(e) for e in data['errors']):
                time.sleep(2); continue
            raise Exception(f"GraphQL fejl: {data['errors']}")
        ext = data.get('extensions', {}).get('cost', {}).get('throttleStatus', {})
        if ext.get('currentlyAvailable', 1000) < 100: time.sleep(1)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        for e in edges:
            sku = e.get('node', {}).get('sku')
            if sku: skus.add(normalize_sku(sku))
        total += len(edges)
        pi = data.get('data', {}).get('productVariants', {}).get('pageInfo', {})
        has_next = pi.get('hasNextPage', False)
        if has_next and edges: cursor = edges[-1].get('cursor')
        if total % 5000 == 0: print(f"   {total:,} varianter hentet...")
    print(f"✅ {len(skus):,} unikke SKUs fra Shopify")
    return skus

# ============================================================
# VIDAXL SCRAPER
# ============================================================

def scrape_vidaxl_variants(url):
    """
    Scrape en VidaXL produktside og udtræk:
    - master_pid: Master produkt ID
    - options: Dict med option-navn -> liste af {value, display} 
    - color_skus: Dict med farve-value -> SKU (fra billede-URLs)
    """
    result = {
        'master_pid': None,
        'options': {},       # attr_name -> {'display_name': str, 'values': [{'value': str, 'display': str}]}
        'color_skus': {},    # color_value -> sku (fra billede-URLs)
        'success': False
    }
    
    try:
        response = requests.get(url, headers=HEADERS_BROWSER, timeout=30)
        if response.status_code != 200:
            return result
        
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        # 1. Find master PID fra enhver data-action-url
        pid_match = re.search(r'pid=([A-Z]\d+)', html)
        if pid_match:
            result['master_pid'] = pid_match.group(1)
        
        if not result['master_pid']:
            # Prøv dwvar_ pattern
            dwvar_match = re.search(r'dwvar_([A-Z]\d+)_', html)
            if dwvar_match:
                result['master_pid'] = dwvar_match.group(1)
        
        # 2. Udtræk FARVE options fra <select> dropdown
        color_select = soup.find('select', {'name': 'color-attribute__value'})
        if color_select:
            color_options = color_select.find_all('option')
            colors = []
            for opt in color_options:
                val = opt.get('value', '')
                if not val: continue
                display = opt.get_text(strip=True)
                # HTML decode
                display = display.replace('&oslash;', 'ø').replace('&aring;', 'å').replace('&aelig;', 'æ')
                colors.append({'value': val, 'display': display})
                
                # Udtræk SKU fra billede-URL
                img_url = opt.get('data-image-url', '')
                sku_in_img = re.search(r'/(\d{5,7})/image_', img_url)
                if sku_in_img:
                    result['color_skus'][val] = sku_in_img.group(1)
            
            if colors:
                result['options']['color'] = {
                    'display_name': 'Farve',
                    'values': colors
                }
        
        # 3. Udtræk ANDRE options fra buttons (størrelse, model, bredde osv.)
        # Find alle variationAttribute labels
        attr_labels = soup.find_all('div', class_=re.compile(r'variationAttribute\d.*font-weight-bold'))
        
        for label_div in attr_labels:
            label_text = label_div.get_text(strip=True)
            # Find klassen for at bestemme attribut-nummeret
            attr_class = [c for c in label_div.get('class', []) if 'variationAttribute' in c]
            if not attr_class: continue
            
            # Find den tilhørende chip-items container (næste søskende)
            chip_container = label_div.find_next('div', class_='chip-items')
            if not chip_container: continue
            
            buttons = chip_container.find_all('button', class_='js-chip-attribute')
            if not buttons: continue
            
            values = []
            attr_name = None
            
            for btn in buttons:
                val = btn.get('data-attr-value', '')
                display = btn.get('data-display-value', '') or btn.get('aria-label', '') or val.replace('_', ' ')
                if val:
                    values.append({'value': val, 'display': display})
                
                # Udtræk attribut-navn fra action URL
                if not attr_name:
                    action_url = btn.get('data-action-url', '')
                    dwvar_attrs = re.findall(r'dwvar_[^_]+_(\w+)=', action_url)
                    # Find den attribut der IKKE er color
                    for a in dwvar_attrs:
                        if a != 'color':
                            attr_name = a
                            break
            
            if values and attr_name:
                # Rens label tekst
                label_clean = re.sub(r'\(\d+ tilgængelige muligheder\)', '', label_text).strip()
                result['options'][attr_name] = {
                    'display_name': label_clean or attr_name,
                    'values': values
                }
        
        result['success'] = True
        
    except Exception as e:
        print(f"   ⚠️ Scrape fejl for {url}: {e}")
    
    return result

# ============================================================
# PRODUKT GRUPPERING
# ============================================================

def match_feed_to_variants(feed_products, scrape_result):
    """
    Match feed-produkter til scraped variant-data.
    Bruger Color-kolonnen fra feed + option values fra scraping.
    Returnerer feed-produkter med tilføjede option-kolonner.
    """
    if not scrape_result['success'] or not scrape_result['options']:
        return feed_products, {}
    
    options_map = {}  # SKU -> {option_name: option_value, ...}
    
    # Hvis vi har color_skus fra scraping, match direkte
    if scrape_result.get('color_skus'):
        sku_to_color = {}
        for color_val, sku in scrape_result['color_skus'].items():
            sku_to_color[normalize_sku(sku)] = color_val
        
        # Match via SKU
        for _, row in feed_products.iterrows():
            sku = normalize_sku(row['SKU'])
            opts = {}
            if sku in sku_to_color:
                color_val = sku_to_color[sku]
                # Find display name
                color_opts = scrape_result['options'].get('color', {}).get('values', [])
                display = next((c['display'] for c in color_opts if c['value'] == color_val), color_val.replace('_', ' '))
                opts['Farve'] = display
            elif pd.notna(row.get('Color')):
                opts['Farve'] = str(row['Color'])
            
            options_map[sku] = opts
    else:
        # Fallback: brug Color-kolonnen fra feed
        for _, row in feed_products.iterrows():
            sku = normalize_sku(row['SKU'])
            opts = {}
            if pd.notna(row.get('Color')):
                opts['Farve'] = str(row['Color'])
            options_map[sku] = opts
    
    # Andre options kan vi ikke matche per SKU uden at scrape hver variant-side
    # Men vi kan bruge titel-forskelle til at udlede størrelse osv.
    # For nu: sæt andre options kun hvis de kan udledes fra feed-data
    
    return feed_products, options_map

# ============================================================
# MATRIXIFY FORMATERING
# ============================================================

def build_matrixify_output(product_groups, config, underkat_config, rum_dict, existing_handles):
    """
    Byg Matrixify CSV fra produktgrupper.
    product_groups: liste af dicts med 'products' (DataFrame), 'options_map', 'common_title'
    """
    rows = []
    handles_used = existing_handles.copy()
    
    for group in product_groups:
        products = group['products']
        options_map = group.get('options_map', {})
        
        if len(products) == 0:
            continue
        
        # Harmoniseret titel
        if len(products) > 1:
            titles = products['Title'].tolist()
            common_title = find_common_title(titles)
        else:
            common_title = clean_vidaxl(products.iloc[0]['Title'])
        
        final_title = title_case_danish(common_title)
        handle = generate_handle(final_title, handles_used)
        
        # Første produkt i gruppen
        first = products.iloc[0]
        hovedkategori = str(first['Category']).split(' > ')[0] if pd.notna(first['Category']) else ''
        
        # Markup og pris config
        cat_config = config[config['Kategori_Config'] == hovedkategori]
        markup = float(cat_config['Markup %'].iloc[0]) if len(cat_config) > 0 and pd.notna(cat_config['Markup %'].iloc[0]) else 70.0
        slutciffer = int(cat_config['Slutciffer'].iloc[0]) if len(cat_config) > 0 and pd.notna(cat_config['Slutciffer'].iloc[0]) else 9
        compare_pct = float(cat_config['Sammenligningspris %'].iloc[0]) if len(cat_config) > 0 and pd.notna(cat_config['Sammenligningspris %'].iloc[0]) else 0
        
        # Check underkategori override
        if not underkat_config.empty:
            cat_str = str(first['Category']).strip() if pd.notna(first['Category']) else ''
            ukat = underkat_config[underkat_config['Underkategori_Config'].astype(str).str.strip() == cat_str]
            if len(ukat) > 0:
                if pd.notna(ukat['Markup %'].iloc[0]): markup = float(ukat['Markup %'].iloc[0])
                if 'Sammenligningspris %' in ukat.columns and pd.notna(ukat['Sammenligningspris %'].iloc[0]):
                    compare_pct = float(ukat['Sammenligningspris %'].iloc[0])
        
        # Find irrelevante options (samme værdi for alle varianter)
        if len(products) > 1:
            all_option_values = defaultdict(set)
            for _, row in products.iterrows():
                sku = normalize_sku(row['SKU'])
                opts = options_map.get(sku, {})
                for k, v in opts.items():
                    all_option_values[k].add(v)
            irrelevant = {k for k, v in all_option_values.items() if len(v) <= 1}
        else:
            irrelevant = set()
        
        # Process hvert produkt i gruppen
        is_first = True
        variant_pos = 0
        
        for _, row in products.iterrows():
            try:
                sku = normalize_sku(row['SKU'])
                
                # Pris
                cost_kr = float(row['B2B price'])
                base_price = cost_kr * (1 + markup / 100)
                price = calculate_price_with_slutciffer(base_price, slutciffer)
                
                c_price = ''
                if compare_pct > 0:
                    c_price = calculate_price_with_slutciffer(price / (1 - compare_pct / 100), slutciffer)
                
                # Tags (kun for hovedprodukt)
                tags_list = []
                if pd.notna(row['Category']):
                    tags_list.extend(extract_hierarchical_tags(row['Category']))
                if pd.notna(row.get('Brand')): tags_list.append(str(row['Brand']))
                if pd.notna(row.get('Color')): tags_list.append(str(row['Color']))
                if 'Parcel_or_pallet' in row.index and pd.notna(row['Parcel_or_pallet']):
                    pv = str(row['Parcel_or_pallet']).strip().lower()
                    if pv == 'parcel': tags_list.append('Parcel')
                    elif pv == 'pallet': tags_list.append('Pallet')
                if rum_dict and pd.notna(row['Category']):
                    cat_str = str(row['Category']).strip()
                    if cat_str in rum_dict and pd.notna(rum_dict[cat_str]):
                        tags_list.append(str(rum_dict[cat_str]))
                
                seen = set()
                tags = ','.join(t for t in tags_list if not (t in seen or seen.add(t)))
                
                # HTML description
                clean_html = clean_vidaxl(row.get('HTML_description', ''))
                
                # Type
                product_type = row['Category'].split(' > ')[-1].strip() if pd.notna(row['Category']) else ''
                
                # SEO
                seo_title = final_title[:70] if len(final_title) <= 70 else final_title[:67] + '...'
                seo_desc = generate_seo_description(clean_html)
                
                # Billeder
                all_images = []
                for i in range(1, 22):
                    if i <= 12: col = f'Image {i}'
                    elif i == 13: col = 'image 13'
                    elif i == 14: col = 'Image 14'
                    else: col = f'image {i}'
                    if col in row.index and pd.notna(row[col]):
                        img = str(row[col]).strip()
                        if validate_url(img): all_images.append(img)
                
                # Vægt
                weight = 0
                if pd.notna(row.get('Weight')):
                    try: weight = int(float(str(row['Weight']).replace(',', '.')) * 1000)
                    except: pass
                
                # Variant position
                variant_pos += 1
                
                # Options (filtrer irrelevante)
                opts = options_map.get(sku, {})
                relevant_opts = {k: v for k, v in opts.items() if k not in irrelevant}
                
                # Byg hovedrække
                product_row = {
                    'Command': 'MERGE',
                    'Handle': handle,
                    'Title': final_title if is_first else '',
                    'Body HTML': clean_html if is_first else '',
                    'Vendor': row.get('Brand', '') if is_first else '',
                    'Type': product_type if is_first else '',
                    'Tags': tags if is_first else '',
                    'Published': 'FALSE',
                    'Status': 'DRAFT',
                    'Variant SKU': sku,
                    'Variant Barcode': str(row.get('EAN', '')),
                    'Variant Position': variant_pos,
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
                    'SEO Title': seo_title if is_first else '',
                    'SEO Description': seo_desc if is_first else '',
                    'Google Shopping / MPN': sku,
                    'Google Shopping / Condition': 'new',
                    'Variant Image': all_images[0] if all_images else '',
                    'Image Src': '',
                    'Image Position': '',
                    'Image Alt Text': ''
                }
                
                # Options
                opt_list = list(relevant_opts.items())
                for i in range(1, 4):
                    if i <= len(opt_list):
                        product_row[f'Option{i} Name'] = opt_list[i-1][0]
                        product_row[f'Option{i} Value'] = opt_list[i-1][1]
                    else:
                        product_row[f'Option{i} Name'] = ''
                        product_row[f'Option{i} Value'] = ''
                
                # Variant metafields (ikke for hovedprodukt)
                if not is_first:
                    product_row['Variant Metafield: custom.produktinfo [multi_line_text_field]'] = clean_html
                    if all_images:
                        product_row['Variant Metafield: custom.variantbilleder [list.single_line_text_field]'] = ', '.join(all_images)
                
                # Billede håndtering
                if is_first and all_images:
                    product_row['Image Src'] = all_images[0]
                    product_row['Image Position'] = '1'
                    product_row['Image Alt Text'] = f"{final_title} - Hovedbillede"
                    rows.append(product_row)
                    
                    # Ekstra billeder
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
                
                is_first = False
                
            except Exception as e:
                print(f"   ⚠️ Fejl ved SKU {row['SKU']}: {str(e)[:100]}")
                continue
    
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ============================================================
# HOVEDPROCESSERING
# ============================================================

try:
    # 1. Hent data
    products = fetch_feed_data(FEED_URL)
    products['SKU'] = products['SKU'].apply(normalize_sku)
    print(f"✅ {len(products):,} produkter i feed")
    
    shopify_skus = fetch_shopify_skus_graphql(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN)
    
    # 2. Læs config
    print(f"\n📋 Læser config...")
    config = pd.read_excel(CONFIG_PATH, sheet_name='Kategori_Config')
    config['Markup %'] = pd.to_numeric(config['Markup %'], errors='coerce')
    config['Slutciffer'] = pd.to_numeric(config['Slutciffer'], errors='coerce')
    config['Sammenligningspris %'] = pd.to_numeric(config['Sammenligningspris %'], errors='coerce')
    
    try:
        underkat_config = pd.read_excel(CONFIG_PATH, sheet_name='Underkategori_Config')
        if 'Markup %' in underkat_config.columns:
            underkat_config['Markup %'] = pd.to_numeric(underkat_config['Markup %'], errors='coerce')
        if 'Sammenligningspris %' in underkat_config.columns:
            underkat_config['Sammenligningspris %'] = pd.to_numeric(underkat_config['Sammenligningspris %'], errors='coerce')
    except:
        underkat_config = pd.DataFrame()
    
    try:
        rum_mapping = pd.read_excel(CONFIG_PATH, sheet_name='Rum_Mapping')
        rum_dict = dict(zip(rum_mapping.iloc[:, 0], rum_mapping.iloc[:, 1]))
    except:
        rum_dict = {}
    
    existing_handles = set()  # Shopify handles - kunne hentes via API men ikke kritisk
    aktive = config[config['Import?'] == 'JA']['Kategori_Config'].tolist()
    print(f"✅ Config loaded - {len(aktive)} aktive kategorier: {', '.join(aktive)}")
    
    # 3. Filtrer nye produkter
    print(f"\n🔍 Filtrerer nye produkter...")
    new_products = products[~products['SKU'].isin(shopify_skus)].copy()
    print(f"   Nye (ikke i Shopify): {len(new_products):,}")
    
    # Kategori filter
    new_products['Hovedkategori'] = new_products['Category'].str.split(' > ').str[0]
    filtered = new_products[new_products['Hovedkategori'].isin(aktive)].copy()
    print(f"   Efter kategori filter: {len(filtered):,}")
    
    # Lager filter
    filtered['Stock'] = pd.to_numeric(filtered['Stock'], errors='coerce').fillna(0)
    filtered = filtered[filtered['Stock'] >= 20].copy()
    print(f"   Efter lager filter (≥20): {len(filtered):,}")
    
    # Pris filter
    filtered['B2B price'] = pd.to_numeric(filtered['B2B price'], errors='coerce').fillna(0)
    filtered = filtered[filtered['B2B price'] > 0].copy()
    print(f"   Efter pris filter (>0): {len(filtered):,}")
    
    if len(filtered) == 0:
        print("\n⚠️ INGEN NYE PRODUKTER! Afslutter.")
        # Gem tom fil
        pd.DataFrame().to_csv('output/matrixify_create.csv', index=False, encoding='utf-8-sig')
        sys.exit(0)
    
    # 4. Vælg batch og scrape VidaXL
    print(f"\n🔍 Scraper VidaXL for variant-data...")
    
    # Tag tilfældig sample af produkter at starte med
    sample_size = min(MAX_PRODUCTS_PER_RUN * 3, len(filtered))  # Oversample da mange vil grupperes
    sample = filtered.sample(n=sample_size, random_state=int(time.time()) % 10000)
    
    # Scrape produktsider og grupper via master PID
    pid_groups = {}       # master_pid -> set af feed-SKUs
    pid_scrape = {}       # master_pid -> scrape_result
    sku_to_pid = {}       # SKU -> master_pid
    scraped_urls = set()
    scrape_count = 0
    
    for _, row in sample.iterrows():
        sku = normalize_sku(row['SKU'])
        
        # Skip hvis allerede grupperet
        if sku in sku_to_pid:
            continue
        
        url = row.get('Link', '')
        if not validate_url(url):
            # Single produkt uden URL
            pid_groups[f"single_{sku}"] = {sku}
            sku_to_pid[sku] = f"single_{sku}"
            continue
        
        if url in scraped_urls:
            continue
        
        # Scrape
        scrape_result = scrape_vidaxl_variants(url)
        scraped_urls.add(url)
        scrape_count += 1
        
        if scrape_result['success'] and scrape_result['master_pid']:
            pid = scrape_result['master_pid']
            pid_scrape[pid] = scrape_result
            
            if pid not in pid_groups:
                pid_groups[pid] = set()
            pid_groups[pid].add(sku)
            sku_to_pid[sku] = pid
            
            # Find alle andre SKUs i feedet med samme master PID
            # Brug color_skus fra scraping
            for color_val, color_sku in scrape_result.get('color_skus', {}).items():
                norm_sku = normalize_sku(color_sku)
                if norm_sku in filtered['SKU'].values:
                    pid_groups[pid].add(norm_sku)
                    sku_to_pid[norm_sku] = pid
        else:
            # Ingen varianter fundet - single produkt
            pid_groups[f"single_{sku}"] = {sku}
            sku_to_pid[sku] = f"single_{sku}"
        
        # Rate limiting
        time.sleep(1)
        
        if scrape_count % 10 == 0:
            print(f"   Scraped {scrape_count} sider, {len(pid_groups)} grupper fundet...")
        
        # Stop når vi har nok grupper
        if len(pid_groups) >= MAX_PRODUCTS_PER_RUN:
            break
    
    print(f"✅ Scraped {scrape_count} sider")
    print(f"   Produktgrupper: {len(pid_groups)}")
    print(f"   Total SKUs i grupper: {sum(len(v) for v in pid_groups.values())}")
    
    # 5. Byg produktgrupper
    print(f"\n📝 Bygger produktgrupper...")
    product_groups = []
    
    for pid, skus in pid_groups.items():
        group_products = filtered[filtered['SKU'].isin(skus)].copy()
        
        if len(group_products) == 0:
            continue
        
        # Match med scrape data
        scrape_data = pid_scrape.get(pid, {'success': False, 'options': {}, 'color_skus': {}})
        group_products, options_map = match_feed_to_variants(group_products, scrape_data)
        
        product_groups.append({
            'pid': pid,
            'products': group_products,
            'options_map': options_map
        })
    
    print(f"✅ {len(product_groups)} produktgrupper klar")
    
    # 6. Generer Matrixify output
    print(f"\n📝 Genererer Matrixify CSV...")
    matrixify = build_matrixify_output(product_groups, config, underkat_config, rum_dict, existing_handles)
    
    if len(matrixify) == 0:
        print("⚠️ Ingen produkter at oprette!")
        pd.DataFrame().to_csv('output/matrixify_create.csv', index=False, encoding='utf-8-sig')
        sys.exit(0)
    
    # 7. Gem
    output_path = 'output/matrixify_create.csv'
    matrixify.to_csv(output_path, index=False, encoding='utf-8-sig', sep=',')
    
    total_products = sum(1 for g in product_groups for _ in range(1))
    total_variants = sum(len(g['products']) for g in product_groups)
    
    print(f"\n✅ SUCCESS!")
    print(f"📁 Fil gemt: {output_path}")
    print(f"📊 {len(matrixify):,} rækker total")
    print(f"   - Produktgrupper: {len(product_groups)}")
    print(f"   - Varianter: {total_variants}")
    print(f"   - Billede rækker: {len(matrixify) - total_variants}")
    
    # Output til GitHub Actions
    github_output = os.environ.get('GITHUB_OUTPUT', '')
    if github_output:
        with open(github_output, 'a') as f:
            f.write(f"product_count={len(product_groups)}\n")
            f.write(f"variant_count={total_variants}\n")
            f.write(f"row_count={len(matrixify)}\n")

except Exception as e:
    print(f"\n❌ FATAL FEJL: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
