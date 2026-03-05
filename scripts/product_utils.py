"""
VidaXL Product Utilities - Delte funktioner for daglig og stor oprettelse
"""
import pandas as pd
import requests
import zipfile
import io
import json
import os
import re
import time
from collections import defaultdict
from bs4 import BeautifulSoup

BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'da-DK,da;q=0.9,en;q=0.8',
}

# ============================================================
# HJÆLPEFUNKTIONER
# ============================================================

def normalize_sku(sku):
    if pd.isna(sku): return ''
    return str(sku).strip().replace('.0', '')

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

def fix_pcs_to_dele(text):
    if pd.isna(text) or not text: return text
    return re.sub(r'\bpcs\b', 'dele', str(text), flags=re.IGNORECASE)

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
    title = fix_pcs_to_dele(title)

    for opt_val in option_values:
        if not opt_val: continue
        opt_str = str(opt_val).strip()

        # Whole-word matching med \b for at undgå substring-matches
        # "Sort" matcher ikke inde i "Sortiment", "1" matcher ikke inde i "105"
        pattern = re.compile(r'\b' + re.escape(opt_str) + r'\b', re.IGNORECASE)
        if pattern.search(title):
            title = pattern.sub(' ', title)
        else:
            # Fuzzy fallback: "X og Y" → match "X og [noget]"
            og_match = re.match(r'^(.+?)\s+og\s+(.+)$', opt_str, re.IGNORECASE)
            if og_match:
                prefix = og_match.group(1).strip()
                fuzzy_pattern = re.compile(
                    r'\b' + re.escape(prefix) + r'\s+og\s+\S+\b',
                    re.IGNORECASE
                )
                title = fuzzy_pattern.sub(' ', title)

        # Hvis fjernet var et tal → fjern klæbeord
        if opt_str.isdigit():
            words = title.split()
            cleaned = [w for w in words if w.lower().rstrip('.,') not in STICKY_AFTER_NUMBER]
            title = ' '.join(cleaned)

    # Fjern kun standalone x der IKKE står mellem tal (bevarer "56 x 54" og "Faux")
    title = re.sub(r'(?<!\d)\s+[xX]\s+(?!\d)', ' ', title)
    # Fjern forældreløse cm/mm der ikke har tal foran
    title = re.sub(r'(?<!\d)\s+[Cc][Mm]\.?\b', '', title)
    title = re.sub(r'(?<!\d)\s+[Mm][Mm]\.?\b', '', title)
    title = re.sub(r'\s+', ' ', title)
    title = title.strip(' ,-–')
    return title

# ============================================================
# BODY HTML FORMATERING
# ============================================================

def _is_spec_bullet(bullet_text):
    if ':' not in bullet_text: return False
    parts = bullet_text.split(':', 1)
    key = parts[0].strip()
    value = parts[1].strip() if len(parts) > 1 else ''
    if len(key) > 40: return False
    if len(value) > 80: return False
    if '.' in value and len(value) > 30: return False
    return True

def _is_warning_text(text):
    lower = text.lower().strip()
    return any(w in lower for w in [
        'advarsel', 'gpsr', 'beskyttelsesudstyr skal',
        'må ikke bruges i trafikken', 'legal document',
        'ikke egnet til børn under'
    ])

def format_body_html(html_description):
    if pd.isna(html_description) or not html_description: return ''
    text = clean_vidaxl(str(html_description))
    has_html_tags = bool(re.search(r'<[a-z]', text, re.IGNORECASE))
    if has_html_tags:
        return _format_html_content(text)
    else:
        return _format_plain_content(text)

def _format_plain_content(text):
    lines = text.split('\n')
    beskrivelse_parts = []
    produktinfo_items = []
    found_split = False
    current_paragraph = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if current_paragraph and not found_split:
                beskrivelse_parts.append(' '.join(current_paragraph))
                current_paragraph = []
            continue

        is_bullet = stripped.startswith('* ') or stripped.startswith('- ')

        if is_bullet:
            bullet_text = stripped[2:].strip()
            if not found_split and _is_spec_bullet(bullet_text):
                if current_paragraph:
                    beskrivelse_parts.append(' '.join(current_paragraph))
                    current_paragraph = []
                found_split = True

            if found_split:
                if bullet_text and not _is_warning_text(bullet_text):
                    produktinfo_items.append(bullet_text)
            else:
                if current_paragraph:
                    beskrivelse_parts.append(' '.join(current_paragraph))
                    current_paragraph = []
                beskrivelse_parts.append(bullet_text)
        else:
            if found_split:
                if not _is_warning_text(stripped):
                    produktinfo_items.append(stripped)
            else:
                current_paragraph.append(stripped)

    if current_paragraph and not found_split:
        beskrivelse_parts.append(' '.join(current_paragraph))

    html_parts = []
    if beskrivelse_parts:
        html_parts.append('<h4>Beskrivelse</h4>')
        for part in beskrivelse_parts:
            if part.strip(): html_parts.append(f'<p>{part.strip()}</p>')

    if produktinfo_items:
        html_parts.append('<h4>ProduktInfo</h4>')
        html_parts.append('<ul>')
        for item in produktinfo_items:
            if item.strip(): html_parts.append(f'<li>{item.strip()}</li>')
        html_parts.append('</ul>')

    result = '\n'.join(html_parts)
    return result if result.strip() else f'<p>{text}</p>'

def _format_html_content(text):
    soup = BeautifulSoup(text, 'html.parser')
    beskrivelse_elems = []
    produktinfo_items = []
    found_split = False

    for elem in soup.children:
        if isinstance(elem, str):
            stripped = elem.strip()
            if stripped and not _is_warning_text(stripped):
                if not found_split: beskrivelse_elems.append(f'<p>{stripped}</p>')
            continue

        tag_name = elem.name if elem.name else ''

        if tag_name == 'ul':
            li_items = elem.find_all('li')
            is_spec_list = any(_is_spec_bullet(li.get_text(strip=True)) for li in li_items)

            if is_spec_list and not found_split:
                found_split = True

            if found_split:
                for li in li_items:
                    li_text = li.get_text(strip=True)
                    if li_text and not _is_warning_text(li_text):
                        inner = re.sub(r'^<li[^>]*>', '', str(li))
                        inner = re.sub(r'</li>$', '', inner).strip()
                        if inner: produktinfo_items.append(inner)
            else:
                beskrivelse_elems.append(str(elem))
        elif tag_name == 'p':
            p_text = elem.get_text(strip=True)
            if p_text and not _is_warning_text(p_text) and not found_split:
                beskrivelse_elems.append(str(elem))
        elif tag_name in ('h1','h2','h3','h4','h5','h6'):
            if not found_split: beskrivelse_elems.append(str(elem))
        else:
            if not found_split:
                elem_text = elem.get_text(strip=True) if hasattr(elem, 'get_text') else str(elem).strip()
                if elem_text and not _is_warning_text(elem_text):
                    beskrivelse_elems.append(str(elem))

    html_parts = []
    if beskrivelse_elems:
        html_parts.append('<h4>Beskrivelse</h4>')
        html_parts.extend(beskrivelse_elems)
    if produktinfo_items:
        html_parts.append('<h4>ProduktInfo</h4>')
        html_parts.append('<ul>')
        for item in produktinfo_items: html_parts.append(f'<li>{item}</li>')
        html_parts.append('</ul>')

    result = '\n'.join(html_parts)
    return result if result.strip() else text

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

def fetch_shopify_data(store, token):
    print(f"\n📥 Henter Shopify SKU→handle map via GraphQL...")
    sku_to_handle = {}
    all_handles = set()
    url = f"https://{store}/admin/api/2024-10/graphql.json"
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    has_next, cursor, total = True, None, 0

    while has_next:
        after = f', after: "{cursor}"' if cursor else ''
        q = '{ productVariants(first: 250%s) { edges { node { sku product { handle } } cursor } pageInfo { hasNextPage } } }' % after
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
            if sku: sku_to_handle[normalize_sku(sku)] = handle
            if handle: all_handles.add(handle)
        total += len(edges)
        pi = data.get('data',{}).get('productVariants',{}).get('pageInfo',{})
        has_next = pi.get('hasNextPage', False)
        if has_next and edges: cursor = edges[-1].get('cursor')
        if total % 5000 == 0: print(f"   {total:,} varianter...")

    print(f"✅ {len(sku_to_handle):,} SKU→handle, {len(all_handles):,} handles")
    return sku_to_handle, all_handles

def fetch_product_options(store, token, handle):
    url = f"https://{store}/admin/api/2024-10/graphql.json"
    headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}
    q = '{ productByHandle(handle: "%s") { options { name position } } }' % handle
    try:
        resp = requests.post(url, headers=headers, json={'query': q}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if 'errors' in data: return []
        product = data.get('data',{}).get('productByHandle')
        if not product: return []
        options = product.get('options', [])
        return [o.get('name','') for o in sorted(options, key=lambda x: x.get('position',0))]
    except:
        return []

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
        if pid_match: result['master_pid'] = pid_match.group(1)
        if not result['master_pid']:
            m = re.search(r'dwvar_([A-Z]\d+)_', html)
            if m: result['master_pid'] = m.group(1)
        if not result['master_pid']:
            result['success'] = True
            return result

        color_select = soup.find('select', {'name': 'color-attribute__value'})
        if color_select:
            colors = []
            for opt in color_select.find_all('option'):
                val = opt.get('value', '')
                if not val: continue
                colors.append({'value': val, 'display': opt.get_text(strip=True)})
            if colors:
                result['options']['color'] = {'display_name': 'Farve', 'values': colors}

        all_action_elems = soup.find_all(attrs={'data-action-url': re.compile('Product-Variation')})
        other_options = {}
        for elem in all_action_elems:
            action_url = elem.get('data-action-url', '')
            attr_value = elem.get('data-attr-value', '')
            if not attr_value: continue
            display_value = (
                elem.get('data-display-value', '') or
                elem.get('aria-label', '') or
                elem.get_text(strip=True) or
                attr_value.replace('_', ' ')
            )
            dwvar_matches = re.findall(r'dwvar_[^_]+_(\w+)=([^&]*)', action_url)
            controlled_attr = None
            for attr_name, url_value in dwvar_matches:
                if attr_name == 'color': continue
                if url_value == attr_value:
                    controlled_attr = attr_name
                    break
            if not controlled_attr: continue
            if controlled_attr not in other_options: other_options[controlled_attr] = []
            if not any(e['value'] == attr_value for e in other_options[controlled_attr]):
                other_options[controlled_attr].append({'value': attr_value, 'display': display_value.strip()})

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


def count_combinations(options):
    """Beregn antal kombinationer for options"""
    total = 1
    for od in options.values():
        total *= max(len(od.get('values', [])), 1)
    return total


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
                except json.JSONDecodeError: pass
            if (i + 1) % 10 == 0: time.sleep(1)
            else: time.sleep(0.3)
        except Exception as e:
            print(f"   ⚠️ API fejl kombination {i+1}: {e}")
            time.sleep(1)

    print(f"   ✅ {len(variant_map)} varianter med SKU")
    return variant_map

# ============================================================
# CONFIG LOADING
# ============================================================

def load_config(config_path):
    config = pd.read_excel(config_path, sheet_name='Kategori_Config')
    config['Markup %'] = pd.to_numeric(config['Markup %'], errors='coerce')
    config['Slutciffer'] = pd.to_numeric(config['Slutciffer'], errors='coerce')
    config['Sammenligningspris %'] = pd.to_numeric(config['Sammenligningspris %'], errors='coerce')

    try:
        underkat = pd.read_excel(config_path, sheet_name='Underkategori_Config')
        if 'Markup %' in underkat.columns: underkat['Markup %'] = pd.to_numeric(underkat['Markup %'], errors='coerce')
        if 'Sammenligningspris %' in underkat.columns: underkat['Sammenligningspris %'] = pd.to_numeric(underkat['Sammenligningspris %'], errors='coerce')
    except: underkat = pd.DataFrame()

    try:
        rum_map = pd.read_excel(config_path, sheet_name='Rum_Mapping')
        rum_dict = dict(zip(rum_map.iloc[:, 0], rum_map.iloc[:, 1]))
    except: rum_dict = {}

    return config, underkat, rum_dict

# ============================================================
# PRICING & TAGS HELPERS
# ============================================================

def get_pricing(row, config, underkat_config):
    hovedkat = str(row['Category']).split(' > ')[0] if pd.notna(row['Category']) else ''
    cat_cfg = config[config['Kategori_Config'] == hovedkat]
    markup = float(cat_cfg['Markup %'].iloc[0]) if len(cat_cfg) > 0 and pd.notna(cat_cfg['Markup %'].iloc[0]) else 70.0
    slutciffer = int(cat_cfg['Slutciffer'].iloc[0]) if len(cat_cfg) > 0 and pd.notna(cat_cfg['Slutciffer'].iloc[0]) else 9
    compare_pct = float(cat_cfg['Sammenligningspris %'].iloc[0]) if len(cat_cfg) > 0 and pd.notna(cat_cfg['Sammenligningspris %'].iloc[0]) else 0

    if not underkat_config.empty:
        cs = str(row['Category']).strip() if pd.notna(row['Category']) else ''
        ukat = underkat_config[underkat_config['Underkategori_Config'].astype(str).str.strip() == cs]
        if len(ukat) > 0:
            if pd.notna(ukat['Markup %'].iloc[0]): markup = float(ukat['Markup %'].iloc[0])
            if 'Sammenligningspris %' in ukat.columns and pd.notna(ukat['Sammenligningspris %'].iloc[0]):
                compare_pct = float(ukat['Sammenligningspris %'].iloc[0])

    return markup, slutciffer, compare_pct


def build_tags(row, rum_dict):
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
    return ','.join(t for t in tags_list if not (t in seen or seen.add(t)))

# ============================================================
# MATRIXIFY OUTPUT — NYE PRODUKTER
# ============================================================

def build_new_products(product_groups, config, underkat, rum_dict, existing_handles, feed):
    rows = []
    handles_used = existing_handles.copy()

    for group in product_groups:
        if group.get('is_merge', False): continue

        feed_rows = group['feed_rows']
        if isinstance(feed_rows, list):
            feed_rows = feed[feed['SKU'].isin(feed_rows)]
        variant_map = group['variant_map']
        option_struct = group.get('options', {})

        if len(feed_rows) == 0: continue

        first = feed_rows.iloc[0]
        markup, slutciffer, compare_pct = get_pricing(first, config, underkat)

        # Titel
        all_opt_displays = set()
        for od in option_struct.values():
            for v in od.get('values', []): all_opt_displays.add(v['display'])
        for _, fr in feed_rows.iterrows():
            if pd.notna(fr.get('Color')): all_opt_displays.add(str(fr['Color']).strip())

        raw_title = str(first['Title']) if pd.notna(first['Title']) else ''
        sorted_displays = sorted(list(all_opt_displays), key=len, reverse=True)
        print(f"   🏷️ Titel: '{raw_title}' → fjerner {len(sorted_displays)} options")
        clean_t = clean_title_from_options(raw_title, sorted_displays)
        final_title = title_case_danish(clean_t)
        if not final_title or len(final_title) < 5:
            final_title = title_case_danish(fix_pcs_to_dele(clean_vidaxl(raw_title)))
        print(f"   🏷️ Resultat: '{final_title}'")

        handle = generate_handle(final_title, handles_used)

        # Irrelevante options
        if len(variant_map) > 1:
            all_ov = defaultdict(set)
            for opts in variant_map.values():
                for k, v in opts.items(): all_ov[k].add(v)
            irrelevant = {k for k, v in all_ov.items() if len(v) <= 1}
        else:
            irrelevant = set()

        is_first = True
        variant_pos = 0

        for _, row in feed_rows.iterrows():
            try:
                sku = normalize_sku(row['SKU'])
                cost_kr = float(row['B2B price'])
                price = calculate_price(cost_kr * (1 + markup / 100), slutciffer)
                c_price = ''
                if compare_pct > 0:
                    c_price = calculate_price(price / (1 - compare_pct / 100), slutciffer)

                tags = build_tags(row, rum_dict)
                body_html = format_body_html(row.get('HTML_description', ''))
                raw_html = clean_vidaxl(row.get('HTML_description', ''))
                product_type = row['Category'].split(' > ')[-1].strip() if pd.notna(row['Category']) else ''
                seo_title = final_title[:70] if len(final_title) <= 70 else final_title[:67] + '...'
                seo_desc = generate_seo_description(body_html)
                all_images = get_all_images(row)

                weight = 0
                if pd.notna(row.get('Weight')):
                    try: weight = int(float(str(row['Weight']).replace(',', '.')) * 1000)
                    except: pass

                variant_pos += 1
                opts = variant_map.get(sku, {})
                relevant = {k: v for k, v in opts.items() if k not in irrelevant}
                opt_list = list(relevant.items())

                product_row = {
                    'Command': 'MERGE', 'Handle': handle,
                    'Title': final_title if is_first else '',
                    'Body HTML': body_html if is_first else '',
                    'Vendor': row.get('Brand', '') if is_first else '',
                    'Type': product_type if is_first else '',
                    'Tags': tags if is_first else '',
                    'Published': 'TRUE' if is_first else '',
                    'Status': 'active' if is_first else '',
                    'Published Scope': 'global' if is_first else '',
                    'Variant SKU': sku,
                    'Variant Barcode': str(row.get('EAN', '')),
                    'Variant Position': variant_pos,
                    'Variant Price': int(price),
                    'Variant Compare At Price': int(c_price) if c_price else '',
                    'Variant Cost': int(cost_kr),
                    'Variant Weight': weight, 'Variant Weight Unit': 'g',
                    'Variant Inventory Tracker': 'shopify',
                    'Variant Inventory Policy': 'deny',
                    'Variant Inventory Qty': int(row.get('Stock', 0) or 0),
                    'Variant Fulfillment Service': 'manual',
                    'Variant Requires Shipping': 'TRUE', 'Variant Taxable': 'TRUE',
                    'SEO Title': seo_title if is_first else '',
                    'SEO Description': seo_desc if is_first else '',
                    'Google Shopping / MPN': sku, 'Google Shopping / Condition': 'new',
                    'Variant Image': all_images[0] if all_images else '',
                    'Image Src': '', 'Image Position': '', 'Image Alt Text': '',
                    'Variant Metafield: custom.sku [single_line_text_field]': sku,
                }

                for i in range(1, 4):
                    if i <= len(opt_list):
                        product_row[f'Option{i} Name'] = opt_list[i-1][0]
                        product_row[f'Option{i} Value'] = opt_list[i-1][1]
                    else:
                        product_row[f'Option{i} Name'] = ''
                        product_row[f'Option{i} Value'] = ''

                if not is_first:
                    product_row['Variant Metafield: custom.produktinfo [multi_line_text_field]'] = raw_html
                    if all_images:
                        product_row['Variant Metafield: custom.variantbilleder [list.single_line_text_field]'] = ', '.join(all_images)

                if is_first and all_images:
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

                is_first = False
            except Exception as e:
                print(f"   ⚠️ Fejl SKU {row.get('SKU','?')}: {str(e)[:100]}")
                continue

    return pd.DataFrame(rows) if rows else pd.DataFrame()

# ============================================================
# MATRIXIFY OUTPUT — MERGE VARIANTER
# ============================================================

def build_merge_variants(product_groups, config, underkat, store, token, feed):
    rows = []

    for group in product_groups:
        if not group.get('is_merge', False): continue

        feed_rows = group['feed_rows']
        if isinstance(feed_rows, list):
            feed_rows = feed[feed['SKU'].isin(feed_rows)]
        variant_map = group['variant_map']
        existing_handle = group['existing_handle']

        if len(feed_rows) == 0 or not existing_handle: continue

        first = feed_rows.iloc[0]
        markup, slutciffer, compare_pct = get_pricing(first, config, underkat)

        existing_option_names = fetch_product_options(store, token, existing_handle)
        if existing_option_names:
            print(f"   📋 Options for {existing_handle}: {existing_option_names}")

        for _, row in feed_rows.iterrows():
            try:
                sku = normalize_sku(row['SKU'])
                cost_kr = float(row['B2B price'])
                price = calculate_price(cost_kr * (1 + markup / 100), slutciffer)
                c_price = ''
                if compare_pct > 0:
                    c_price = calculate_price(price / (1 - compare_pct / 100), slutciffer)

                raw_html = clean_vidaxl(row.get('HTML_description', ''))
                all_images = get_all_images(row)

                weight = 0
                if pd.notna(row.get('Weight')):
                    try: weight = int(float(str(row['Weight']).replace(',', '.')) * 1000)
                    except: pass

                opts = variant_map.get(sku, {})
                ordered_opts = []
                if existing_option_names:
                    for opt_name in existing_option_names:
                        if opt_name in opts: ordered_opts.append((opt_name, opts[opt_name]))
                    for k, v in opts.items():
                        if k not in existing_option_names: ordered_opts.append((k, v))
                else:
                    ordered_opts = list(opts.items())

                merge_row = {
                    'Handle': existing_handle, 'Variant Command': 'MERGE',
                    'Variant SKU': sku, 'Variant Barcode': str(row.get('EAN', '')),
                    'Variant Price': int(price),
                    'Variant Compare At Price': int(c_price) if c_price else '',
                    'Variant Cost': int(cost_kr),
                    'Variant Weight': weight, 'Variant Weight Unit': 'g',
                    'Variant Inventory Tracker': 'shopify', 'Variant Inventory Policy': 'deny',
                    'Variant Inventory Qty': int(row.get('Stock', 0) or 0),
                    'Variant Fulfillment Service': 'manual',
                    'Variant Requires Shipping': 'TRUE', 'Variant Taxable': 'TRUE',
                    'Variant Image': all_images[0] if all_images else '',
                    'Google Shopping / MPN': sku, 'Google Shopping / Condition': 'new',
                    'Variant Metafield: custom.sku [single_line_text_field]': sku,
                    'Variant Metafield: custom.produktinfo [multi_line_text_field]': raw_html,
                }
                if all_images:
                    merge_row['Variant Metafield: custom.variantbilleder [list.single_line_text_field]'] = ', '.join(all_images)

                for i in range(1, 4):
                    if i <= len(ordered_opts):
                        merge_row[f'Option{i} Name'] = ordered_opts[i-1][0]
                        merge_row[f'Option{i} Value'] = ordered_opts[i-1][1]
                    else:
                        merge_row[f'Option{i} Name'] = ''
                        merge_row[f'Option{i} Value'] = ''

                rows.append(merge_row)
            except Exception as e:
                print(f"   ⚠️ Merge fejl SKU {row.get('SKU','?')}: {str(e)[:100]}")
                continue

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def save_xlsx(df, path, sheet_name='Products'):
    if len(df) > 0:
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    else:
        pd.DataFrame().to_excel(path, index=False, engine='openpyxl', sheet_name=sheet_name)
