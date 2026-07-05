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
import sys
import time
import math
import random
from datetime import datetime, timedelta, timezone

# Shared pricing module (tier-based markup, sale rounding, group assignment)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing
# Kanonisk titel-motor (100%-valideret 2026-07-05) — samme logik som merge-oraklet,
# så nyoprettede + merge-tilføjede produkter får titler konsistente med det merge-eksekverede katalog.
from title_engine import generate_title, titlecase_feed

# Number of days a new product stays in 'warmup' status before it can join a sale rotation.
# Matches Omnibus 30-day reference + 4.4-rule (sale max half of normalpris-period).
WARMUP_DAYS = 60

# Supabase config loading (fallback til lokal XLSX)
def _load_supabase_config():
    """Forsog at hente kategori-config fra Supabase hub_settings."""
    try:
        url = os.environ.get('SUPABASE_URL')
        key = os.environ.get('SUPABASE_SERVICE_KEY')
        if not url or not key:
            return None
        from supabase import create_client
        sb = create_client(url, key)
        keys = ['product_automation_categories', 'product_automation_subcategory_overrides', 'vidaxl_rum_mapping', 'product_automation_pricing']
        res = sb.table('hub_settings').select('key, value').in_('key', keys).execute()
        if not res.data:
            return None
        data_map = {r['key']: r['value'] for r in res.data}
        categories = data_map.get('product_automation_categories')
        if not categories or not isinstance(categories, list) or len(categories) == 0:
            return None
        config_rows = []
        for cat in categories:
            config_rows.append({
                'Kategori_Config': cat.get('kategori', ''),
                'Import?': 'JA' if cat.get('aktiv', True) else 'NEJ',
            })
        # Underkategori overrides fra hub
        underkat_rows = []
        sub_overrides = data_map.get('product_automation_subcategory_overrides', [])
        if isinstance(sub_overrides, list):
            for ov in sub_overrides:
                path = ov.get('path', '')
                handling = ov.get('handling', 'TILFØJ')
                if not path:
                    continue
                row = {
                    'Underkategori_Config': path,
                    'Handling': handling,
                }
                underkat_rows.append(row)
        # Rum mapping
        rum_dict = data_map.get('vidaxl_rum_mapping', {})
        if not isinstance(rum_dict, dict):
            rum_dict = {}
        # Pris tabel
        pricing_tiers = data_map.get('product_automation_pricing', [])
        if isinstance(pricing_tiers, list) and pricing_tiers:
            pris_rows = [{'Indkøb': t.get('indkob', 0), 'Markup': t.get('markup', 1.7)} for t in pricing_tiers]
            pris_df = pd.DataFrame(pris_rows)
        else:
            pris_df = pd.DataFrame()

        config_df = pd.DataFrame(config_rows)
        underkat_df = pd.DataFrame(underkat_rows) if underkat_rows else pd.DataFrame()
        if not pris_df.empty:
            pris_df['Indkøb'] = pd.to_numeric(pris_df['Indkøb'], errors='coerce')
            pris_df['Markup'] = pd.to_numeric(pris_df['Markup'], errors='coerce')
            pris_df = pris_df.sort_values('Indkøb').reset_index(drop=True)
        print(f"[CONFIG] Loaded {len(config_rows)} kategorier + {len(underkat_rows)} underkategori-overrides + {len(rum_dict)} rum-mappings + {len(pris_df)} pristrin fra Supabase")
        return config_df, underkat_df, rum_dict, pris_df
    except Exception as e:
        print(f"[CONFIG] Supabase fejl, falder tilbage til XLSX: {e}")
        return None
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

def calculate_price(raw_price):
    """Afrund op til nærmeste 50, minus 1. Eks: 265 → 300 → 299"""
    return int(math.ceil(raw_price / 50) * 50 - 1)

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
        if opt_str.isdigit():
            # For tal: fjern tallet + evt. klæbeord direkte efter (f.eks. "6 sæt", "4 stk.")
            sticky_words = [s.rstrip('.') for s in STICKY_AFTER_NUMBER]
            sticky_pattern = '|'.join(re.escape(s) for s in sticky_words)
            pattern = re.compile(
                r'\b' + re.escape(opt_str) + r'(?:\s+(?:' + sticky_pattern + r')\.?)?(?=\s|$)',
                re.IGNORECASE
            )
            title = pattern.sub(' ', title)
        else:
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

    # Fjern kun standalone x der IKKE står mellem tal (bevarer "56 x 54" og "Faux")
    title = re.sub(r'(?<!\d)\s+[xX]\s+(?!\d)', ' ', title)
    # Fjern forældreløse cm/mm der ikke har tal foran
    title = re.sub(r'(?<!\d)\s+[Cc][Mm]\.?\b', '', title)
    title = re.sub(r'(?<!\d)\s+[Mm][Mm]\.?\b', '', title)
    # Fjern kommaer der IKKE staar mellem cifre (bevarer danske decimalkommaer som "1,5")
    # Eksempel: "Polyrattan, Staal Og Massivt Akacietrae" -> "Polyrattan Staal Og Massivt Akacietrae"
    title = re.sub(r'(?<!\d),(?!\d)', '', title)
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
# VARIANT-OPTIONS via item_variant (pålidelig — erstatter combo-scrape)
# ============================================================
# Baggrund (2026-07-05): fetch_variant_skus (combo-enumering mod Product-Variation-
# endpoint) fejler for omstrukturerede masters (returnerer master uden SKU → 0 varianter).
# Item_variant-metoden er robust: søster-SKUs fra master_pid-mappingen (Supabase) +
# hver SKUs egne options fra 'item_variant'-JSON på dens produktside.
import html as _htmlmod

def _item_variant_of(sku, url):
    try:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=25)
        if r.status_code != 200:
            return None
        txt = _htmlmod.unescape(r.text)
        m = (re.search(r'"sku":"' + re.escape(str(sku)) + r'","item_variant":(\{.*?\})', txt)
             or re.search(r'"item_variant":(\{.*?\})', txt))
        return json.loads(m.group(1)) if m else None
    except Exception:
        return None

def _siblings_of_master(master_pid):
    """Alle SKUs på en master fra vidaxl_sku_master (Supabase)."""
    import urllib.request
    base = os.environ.get('SUPABASE_URL') or os.environ.get('NEXT_PUBLIC_SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_KEY')
    if not base or not key:
        return []
    try:
        req = urllib.request.Request(
            f"{base.rstrip('/')}/rest/v1/vidaxl_sku_master?select=sku&master_pid=eq.{master_pid}",
            headers={'apikey': key, 'Authorization': f'Bearer {key}'})
        return [str(r['sku']).strip() for r in json.loads(urllib.request.urlopen(req, timeout=30).read())]
    except Exception:
        return []

def fetch_variant_options_v2(master_pid, scrape_options, links_by_sku):
    """Pålidelig variant-map via item_variant. Returnerer {sku: {dansk_option_navn: værdi}}
    — samme shape som fetch_variant_skus. Søster-SKUs fra master_pid-mappingen, options
    fra hver SKUs item_variant. Danske akse-navne fra scrape_options' display_name."""
    name_map = {k: v.get('display_name', k) for k, v in (scrape_options or {}).items()}
    out = {}
    for s in _siblings_of_master(master_pid):
        url = links_by_sku.get(s)
        if not url:
            continue
        iv = _item_variant_of(s, url)
        if not iv:
            continue
        ov = {name_map.get(k, k): val for k, val in iv.items() if val}
        if ov:
            out[s] = ov
        time.sleep(0.15)
    return out


# ============================================================
# CONFIG LOADING
# ============================================================

def load_config(config_path):
    # Forsog Supabase forst (hub-styret config)
    sb_result = _load_supabase_config()
    if sb_result is not None:
        return sb_result
    print(f"[CONFIG] Bruger lokal XLSX: {config_path}")
    config = pd.read_excel(config_path, sheet_name='Kategori_Config')

    try:
        underkat = pd.read_excel(config_path, sheet_name='Underkategori_Config')
    except: underkat = pd.DataFrame()

    try:
        rum_map = pd.read_excel(config_path, sheet_name='Rum_Mapping')
        rum_dict = dict(zip(rum_map.iloc[:, 0], rum_map.iloc[:, 1]))
    except: rum_dict = {}

    try:
        pris_df = pd.read_excel(config_path, sheet_name='Pris_Config')
        pris_df['Indkøb'] = pd.to_numeric(pris_df['Indkøb'], errors='coerce')
        pris_df['Markup'] = pd.to_numeric(pris_df['Markup'], errors='coerce')
        pris_df = pris_df.sort_values('Indkøb').reset_index(drop=True)
        print(f"[CONFIG] {len(pris_df)} pristrin loaded")
    except:
        pris_df = pd.DataFrame()
        print("[CONFIG] ⚠️ Ingen Pris_Config sheet fundet, bruger fallback 1.7x")

    return config, underkat, rum_dict, pris_df

# ============================================================
# PRICING & TAGS HELPERS
# ============================================================

def get_markup(b2b_price, pris_config):
    """Slå markup-multiplikator op baseret på indkøbspris.
    Pristabellen definerer intervaller: 100 = 1-100, 200 = 101-200, osv.
    Over højeste trin bruges 1.7x (flat)."""
    if pris_config.empty:
        return 1.7  # Fallback
    b2b = float(b2b_price)
    # Find det trin hvor Indkøb >= b2b (laveste interval der dækker prisen)
    matching = pris_config[pris_config['Indkøb'] >= b2b]
    if len(matching) > 0:
        return float(matching.iloc[0]['Markup'])
    # Over højeste trin → brug sidste trin (1.7x)
    return float(pris_config.iloc[-1]['Markup'])


def calculate_compare_price(selling_price):
    """Beregn sammenligningspris: random 15-35% rabat, ceil-50-minus-1, max 9999."""
    discount_pct = random.uniform(0.15, 0.35)
    raw_compare = selling_price / (1 - discount_pct)
    compare = calculate_price(raw_compare)
    # Cap ved 9999 medmindre udsalgspris > 9800
    if selling_price <= 9800 and compare > 9999:
        compare = 9999
    return compare


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

def build_new_products(product_groups, config, underkat, rum_dict, existing_handles, feed, pricing_cfg=None):
    """Build Matrixify rows for entirely new products.

    New products are created WITHOUT compare_at_price (warmup state) — they are
    inserted into vidaxl_pricing_state with status='warmup' so they get picked up
    in the next rotation cycle of their group once warmup_complete_at has passed.

    Returns (DataFrame, list_of_state_records).
    """
    rows = []
    state_records = []
    warmup_until = (datetime.now(timezone.utc) + timedelta(days=WARMUP_DAYS)).isoformat()
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

        # === TITEL via kanonisk motor (title_engine) ===
        # Kun REELLE variant-akser (>1 distinkt værdi) fjernes fra titlen — så en
        # enkelt-farvet/enkelt-størrelse-vare beholder sin identitet (matcher create-flowets
        # egen 'irrelevant'-filtrering). Feed-Color kun som strip-hint ved >1 farve.
        raw_title = str(first['Title']) if pd.notna(first['Title']) else ''
        _axis_vals = defaultdict(set)
        for _ov in variant_map.values():
            for _k, _v in _ov.items():
                if _v: _axis_vals[_k].add(_v)
        title_opts = {k: sorted(vs) for k, vs in _axis_vals.items() if len(vs) > 1}
        _fc = [str(fr['Color']).strip() for _, fr in feed_rows.iterrows()
               if pd.notna(fr.get('Color')) and str(fr['Color']).strip()]
        title_feed_colors = _fc if len({c.lower() for c in _fc}) > 1 else []
        _ptype = str(first['Category']).split(' > ')[-1].strip() if pd.notna(first.get('Category')) else ''
        final_title, _needs_llm = generate_title(
            titlecase_feed(raw_title), title_opts, shared=True,
            product_type=_ptype, feed_colors=title_feed_colors, n_variants=len(feed_rows))
        if _needs_llm:
            print(f"   ⚠️ needs_llm={_needs_llm} (usædvanlig feed-titel): {raw_title!r}")
        if not final_title or len(final_title) < 3:
            final_title = title_case_danish(fix_pcs_to_dele(clean_vidaxl(raw_title)))
        print(f"   🏷️ Titel: {raw_title!r} → {final_title!r}")

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
                price = pricing.calculate_normal_price(cost_kr, pricing_cfg)
                sale_price = pricing.calculate_sale_price(cost_kr, pricing_cfg)
                pricing_group = pricing.assign_group(sku)

                state_records.append({
                    'sku': sku,
                    'pricing_group': pricing_group,
                    'status': 'warmup',
                    'b2b_cost': cost_kr,
                    'normal_price': price,
                    'sale_price': sale_price,
                    'warmup_complete_at': warmup_until,
                })

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
                    'Variant Compare At Price': '',
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

    return (pd.DataFrame(rows) if rows else pd.DataFrame(), state_records)

# ============================================================
# MATRIXIFY OUTPUT — MERGE VARIANTER
# ============================================================

def _build_merge_row(handle, sku, row, ordered_opts, pricing_cfg):
    """Byg én merge-række fra feed-data. Returnerer (merge_row, state_record)."""
    cost_kr = float(row['B2B price'])
    price = pricing.calculate_normal_price(cost_kr, pricing_cfg)
    sale_price = pricing.calculate_sale_price(cost_kr, pricing_cfg)
    pricing_group = pricing.assign_group(sku)
    warmup_until = (datetime.now(timezone.utc) + timedelta(days=WARMUP_DAYS)).isoformat()

    state_record = {
        'sku': sku,
        'pricing_group': pricing_group,
        'status': 'warmup',
        'b2b_cost': cost_kr,
        'normal_price': price,
        'sale_price': sale_price,
        'warmup_complete_at': warmup_until,
    }

    raw_html = clean_vidaxl(row.get('HTML_description', ''))
    all_images = get_all_images(row)

    weight = 0
    if pd.notna(row.get('Weight')):
        try: weight = int(float(str(row['Weight']).replace(',', '.')) * 1000)
        except: pass

    merge_row = {
        'Handle': handle, 'Variant Command': 'MERGE',
        'Variant SKU': sku, 'Variant Barcode': str(row.get('EAN', '')),
        'Variant Price': int(price),
        'Variant Compare At Price': '',
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

    return (merge_row, state_record)


def build_merge_variants(product_groups, config, underkat, store, token, feed, pricing_cfg=None):
    """Build merge rows for new variants on existing products.

    Like build_new_products, new variants are created with no compare_at_price
    and inserted into vidaxl_pricing_state with status='warmup'.

    Returns (DataFrame, list_of_state_records).
    """
    rows = []
    state_records = []

    # Byg feed lookup
    feed_by_sku = {}
    for _, r in feed.iterrows():
        s = normalize_sku(r['SKU'])
        if s and s not in feed_by_sku: feed_by_sku[s] = r

    for group in product_groups:
        if not group.get('is_merge', False): continue

        feed_rows = group['feed_rows']
        if isinstance(feed_rows, list):
            feed_rows = feed[feed['SKU'].isin(feed_rows)]
        variant_map = group['variant_map']
        existing_handle = group['existing_handle']
        existing_skus = group.get('existing_skus', [])
        all_variant_map = group.get('all_variant_map', {})

        # Hent eksisterende options fra Shopify
        existing_option_names = fetch_product_options(store, token, existing_handle)
        if existing_option_names:
            print(f"   📋 Options for {existing_handle}: {existing_option_names}")

        # Check om nye varianter har options der ikke findes på produktet
        new_option_names = set()
        for opts in variant_map.values():
            new_option_names.update(opts.keys())

        needs_option_update = bool(new_option_names - set(existing_option_names)) if existing_option_names else False

        if needs_option_update:
            missing_opts = new_option_names - set(existing_option_names)
            print(f"   🔄 Nye options nødvendige: {missing_opts} → inkluderer eksisterende varianter")

        # Hjælper: byg ordered options fra variant_map entry
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

        # 1. Hvis nye options → inkludér eksisterende varianter med feed-data
        if needs_option_update and existing_skus:
            for ex_sku in existing_skus:
                try:
                    # Hent feed-data
                    if ex_sku not in feed_by_sku: continue
                    ex_row = feed_by_sku[ex_sku]

                    # Hent options fra all_variant_map (scraped data)
                    ex_opts = all_variant_map.get(ex_sku, {})
                    if not ex_opts: continue

                    ordered = order_opts(ex_opts)
                    merge_row, state_record = _build_merge_row(existing_handle, ex_sku, ex_row, ordered, pricing_cfg)
                    rows.append(merge_row)
                    state_records.append(state_record)
                    print(f"   📝 Eksisterende variant {ex_sku} opdateret med nye options")
                except Exception as e:
                    print(f"   ⚠️ Fejl eksist. SKU {ex_sku}: {str(e)[:100]}")

        # 2. Nye varianter (som altid)
        for _, row in feed_rows.iterrows():
            try:
                sku = normalize_sku(row['SKU'])
                opts = variant_map.get(sku, {})
                ordered = order_opts(opts)
                merge_row, state_record = _build_merge_row(existing_handle, sku, row, ordered, pricing_cfg)
                rows.append(merge_row)
                state_records.append(state_record)
            except Exception as e:
                print(f"   ⚠️ Merge fejl SKU {row.get('SKU','?')}: {str(e)[:100]}")
                continue

        # 3. TITEL-REGEN: når en NY akse tilføjes, kan den delte titel være blevet forkert
        #    (fx enkelt-farvet produkt får en 2. farve → farven skal UD af titlen). Regenerér
        #    fra hele det merged option-sæt (eksisterende + nye varianter) via samme motor.
        #    Emit'es som produkt-niveau række (Handle+Title) i samme output.
        if needs_option_update:
            try:
                merged_axis = defaultdict(set)
                for ov in list(all_variant_map.values()) + list(variant_map.values()):
                    for k, v in ov.items():
                        if v: merged_axis[k].add(v)
                t_opts = {k: sorted(vs) for k, vs in merged_axis.items() if len(vs) > 1}
                all_skus = list(existing_skus) + [normalize_sku(r['SKU']) for _, r in feed_rows.iterrows()]
                rep_row = next((feed_by_sku[s] for s in all_skus if s in feed_by_sku), None)
                if rep_row is not None:
                    rep_title = str(rep_row['Title']) if pd.notna(rep_row.get('Title')) else ''
                    _fcs = [str(feed_by_sku[s]['Color']).strip() for s in all_skus
                            if s in feed_by_sku and pd.notna(feed_by_sku[s].get('Color'))
                            and str(feed_by_sku[s]['Color']).strip()]
                    _fcs = _fcs if len({c.lower() for c in _fcs}) > 1 else []
                    _pt = str(rep_row['Category']).split(' > ')[-1].strip() if pd.notna(rep_row.get('Category')) else ''
                    new_title, _ = generate_title(titlecase_feed(rep_title), t_opts, shared=True,
                                                  product_type=_pt, feed_colors=_fcs,
                                                  n_variants=max(len(all_skus), 2))
                    if new_title and len(new_title) >= 3:
                        rows.append({'Handle': existing_handle, 'Command': 'MERGE', 'Title': new_title})
                        print(f"   🏷️ Titel-regen (ny akse på {existing_handle}) → {new_title!r}")
            except Exception as e:
                print(f"   ⚠️ Titel-regen fejl {existing_handle}: {str(e)[:100]}")

    return (pd.DataFrame(rows) if rows else pd.DataFrame(), state_records)


def upsert_warmup_state(state_records, supabase=None):
    """Idempotently insert warmup state rows into vidaxl_pricing_state.

    Uses ON CONFLICT (sku) DO NOTHING semantics so that re-runs / merge variants
    that already have a state row are NEVER overwritten — protects existing
    on_sale / normal status from being reset to warmup.
    """
    if not state_records:
        return 0
    if supabase is None:
        url = os.environ.get('SUPABASE_URL')
        key = os.environ.get('SUPABASE_SERVICE_KEY')
        if not url or not key:
            print("[WARMUP] Mangler SUPABASE_URL/SUPABASE_SERVICE_KEY — kan ikke skrive state")
            return 0
        try:
            from supabase import create_client
            supabase = create_client(url, key)
        except Exception as e:
            print(f"[WARMUP] Supabase init fejlede: {e}")
            return 0
    try:
        res = supabase.table('vidaxl_pricing_state').upsert(
            state_records, on_conflict='sku', ignore_duplicates=True
        ).execute()
        inserted = len(res.data) if res.data else 0
        print(f"[WARMUP] Indsat {inserted} nye state-rækker (af {len(state_records)} forsøgt)")
        return inserted
    except Exception as e:
        print(f"[WARMUP] Upsert fejlede: {e}")
        return 0


def save_xlsx(df, path, sheet_name='Products'):
    if len(df) > 0:
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    else:
        pd.DataFrame().to_excel(path, index=False, engine='openpyxl', sheet_name=sheet_name)
