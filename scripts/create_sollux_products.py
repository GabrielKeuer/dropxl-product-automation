"""Opret Sollux-produkter i Shopify via GraphQL productSet (uden Matrixify).

Selvstændigt Sollux-modul (isoleret fra det daglige vidaXL-flow). Genbruger
SAMME productSet-mønster som create_products_v2.py, tilpasset Sollux:
  - Feed: CSV_dunski (UTF-8) + kostpris fra prisliste + lager fra availability
  - farve-variant-gruppering (serie-navn = STORE bogstaver)
  - flad prisformel: pris = round9(RRP_EUR×7,45); cost = RRP/1,20×0,55×7,45
  - struktur strømlinet mod EKSISTERENDE Shopify-Sollux:
      * option "Farve" (Title-Case), tags med \\xa0-præfiks (undt. tabsmenu),
        farve-tag lowercase, serie-tag Title-Case
      * K-rum-tags (kerne K6/K7/K8 + K2/K5; Skrivebord K4)
      * custom.anbefalet_paere (fatning → fast pære-GID) til mersalg
      * billed-rotation 3→1,1→2,2→3
      * SEO genereret af Claude i BoligRetning-format
      * variant-metafelter custom.sku/produktinfo/variantbilleder

Brug:
  python create_sollux_products.py --dry-run
  python create_sollux_products.py --live --limit 10 --status DRAFT
Env: SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, ANTHROPIC_API_KEY, (valgfri) LOCATION_ID, FEED_DIR
"""
import argparse, json, os, re, sys, time
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing  # delt pris-motor (round_9 + flat_surcharge tilføjet for Sollux)

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', 'b7916a-38.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
LOCATION_ID = os.environ.get('LOCATION_ID', '97768178013')
EUR_TO_DKK = 7.45
API_VERSION = '2024-10'
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN, 'Content-Type': 'application/json'}
NBSP = '\xa0'

FEED_DIR = os.environ.get('FEED_DIR', r'C:\Users\APC\Downloads')
DUNSKI = os.path.join(FEED_DIR, 'CSV_dunski_EUR_wszystkie.csv')
PRICELIST = os.path.join(FEED_DIR, 'SOLLUX_PRICELIST_EUR_EN.xls')
AVAILABILITY = os.path.join(FEED_DIR, 'products_availability.csv')

# LIVE feed-URLs (fra Sollux 2026-06-30). Brug --fetch (GitHub Actions/månedligt job).
FEED_URLS = {
    'CSV_dunski_EUR_wszystkie.csv': 'https://sollux-lighting.com/product_feed/sollux/CSV_dunski_EUR_wszystkie.csv',
    'products_availability.csv': 'https://apps.sollux-lighting.com/stock/products_availability.csv',
    'SOLLUX_PRICELIST_EUR_EN.xls': 'https://sollux-lighting.com/cenniki2022/SOLLUX_PRICELIST_EUR_EN.xls',
}

def fetch_live_feeds(dest):
    """Hent de 3 feed-filer live. Retry (Sollux blokerer tidvis GitHub-runner-IP'er)."""
    os.makedirs(dest, exist_ok=True)
    ua = {'User-Agent': 'Mozilla/5.0 (compatible; BoligRetning/1.0)'}
    for fn, url in FEED_URLS.items():
        for attempt in range(1, 6):
            try:
                r = requests.get(url, timeout=120, headers=ua)
                r.raise_for_status()
                with open(os.path.join(dest, fn), 'wb') as f:
                    f.write(r.content)
                print(f"  hentet {fn} ({len(r.content):,} bytes)")
                break
            except Exception as e:
                if attempt == 5:
                    raise
                time.sleep(2 ** attempt)
IMAGE_BASE = 'https://sollux-lighting.com/sollux-all-baselinker'

TEXT_FIXES = {'cooper': 'kobber', 'Cooper': 'Kobber',
              'vedhæng lampe': 'pendellampe', 'Vedhæng lampe': 'Pendellampe'}

# --- Strømlining mod eksisterende Shopify-taksonomi ---
TYPE_MAP = {
    'Loftlamper': 'Loftlamper', 'Hængende lamper': 'Hængende lamper',
    'Væglamper': 'Væglamper', 'Lysekroner': 'Lysekroner',
    'Skrivebordslamper': 'Skrivebordslamper',
    'Żarówki': 'Lyskilde', 'Zarówki': 'Lyskilde', 'Lyskilde': 'Lyskilde',
    'Lampy podłogowe': 'Gulvlamper', 'Gulvlamper': 'Gulvlamper',
}
EXISTING_COLORS = ['Hvid', 'Sort', 'Grå', 'Sort/Guld', 'Sort/Krom', 'Sort/Kobber',
                   'Krom', 'Beige', 'Guld', 'Grøn', 'Gul', 'Beton', 'Orange',
                   'Træ', 'Sølv', 'Rød', 'Olivengrøn', 'Rød okker']
_EXIST_EXACT = {c.lower(): c for c in EXISTING_COLORS}
_EXIST_SEP = {re.sub(r'[ /\-]+', '/', c.lower()): c for c in EXISTING_COLORS}

# anbefalet_paere: fatning -> fast pære-produkt-GID (udledt 100% fra eksisterende)
FATNING_TO_BULB = {
    'E27': 'gid://shopify/Product/15712549110109',
    'E14': 'gid://shopify/Product/15713519436125',
    'G9':  'gid://shopify/Product/15712549765469',
    'GU10': 'gid://shopify/Product/15713519698269',
    'GU10/ES111': 'gid://shopify/Product/15713519698269',
}
ND = {'', 'n/d', 'nan', 'none', 'null', '-'}

# Pris-config hentes fra hub (pricing_rules). Fallback hvis hub utilgængelig (matcher DB).
_LAMP_FALLBACK = {"mode": "fictive_discount", "fixed_markup": 1.20/0.55, "rounding": "round_9", "fictive_discounts": [20, 25, 30, 35]}
_BULB_FALLBACK = {"mode": "fictive_discount", "fixed_markup": 1.20/0.55, "rounding": "round_9", "fictive_discounts": [], "flat_surcharge": 10}

def load_sollux_configs():
    lamp = pricing.load_pricing_config(vendor='Sollux', product_type=None)
    bulb = pricing.load_pricing_config(vendor='Sollux', product_type='Lyskilde')
    src = 'hub' if (lamp and bulb) else 'fallback'
    return (lamp or _LAMP_FALLBACK), (bulb or _BULB_FALLBACK), src

def round_to_9(price):
    price = int(price)
    return price if price % 10 == 9 else ((price // 10) + 1) * 10 - 1

def convert_danish_chars(text):
    if pd.isna(text): return ''
    text = str(text)
    for d, e in {'æ':'ae','Æ':'ae','ø':'oe','Ø':'oe','å':'aa','Å':'aa','ä':'ae','ö':'oe','ü':'ue'}.items():
        text = text.replace(d, e)
    return text

def generate_handle(title, used):
    h = convert_danish_chars(str(title).lower())
    h = re.sub(r'[^a-z0-9]+', '-', h).strip('-')
    while '--' in h: h = h.replace('--', '-')
    o, n = h, 1
    while h in used: h = f"{o}-{n}"; n += 1
    used.add(h)
    return h

def norm_color(c):
    if pd.isna(c): return None
    cl = str(c).strip().lower()
    if not cl or cl in ('ikke anvendelig', 'ikke relevant'): return None
    if cl in _EXIST_EXACT: return _EXIST_EXACT[cl]
    sep = re.sub(r'[ \-]+', '/', cl)
    if sep in _EXIST_SEP: return _EXIST_SEP[sep]
    return cl[:1].upper() + cl[1:]

def raw_color(c):
    """Lowercase rå farve til TAG (matcher eksisterende \\xa0sort/krom-stil)."""
    if pd.isna(c): return None
    cl = str(c).strip().lower()
    return None if (not cl or cl in ('ikke anvendelig', 'ikke relevant')) else cl

def map_type(kat):
    if pd.isna(kat): return ('Belysning', True)
    k = str(kat).strip()
    if k in TYPE_MAP: return (TYPE_MAP[k], TYPE_MAP[k] == 'Gulvlamper')
    return (k, True)

def base_from_name_color(name, color=None):
    if pd.isna(name): return ''
    words = str(name).strip().split()
    last = -1
    for i, w in enumerate(words):
        wc = w.strip('.,')
        if (len(wc) >= 2 and any(c.isupper() for c in wc) and not any(c.islower() for c in wc)) \
           or re.fullmatch(r'\d+([.,]\d+)?', wc):
            last = i
    return ' '.join(words[:last + 1]) if last >= 0 else ' '.join(words)

def extract_series(name):
    """Serie-navn = sidste rene STORE-bogstavs-ord (NERO->Nero) til serie-tag."""
    if pd.isna(name): return None
    ser = None
    for w in str(name).split():
        wc = w.strip('.,')
        if len(wc) >= 2 and wc.isalpha() and wc.isupper():
            ser = wc
    return ser.title() if ser else None

def apply_fixes(text, fixes):
    if pd.isna(text): return text
    t = str(text)
    for a, b in fixes.items(): t = t.replace(a, b)
    return t

def is_nd(v):
    return pd.isna(v) or str(v).strip().lower() in ND

def fatning_of(row):
    for col in ('Trådtype', 'Forbindelse'):
        v = row.get(col)
        if not is_nd(v) and str(v).strip().lower() not in ('ikke anvendelig',):
            return str(v).strip()
    return None

def _img_num(url):
    """Billed-nummer fra filnavn '…-{n}-k.jpg' (fx 0256-3-k.jpg -> 3)."""
    m = re.search(r'-(\d+)-k\.jpg', str(url), re.I)
    return int(m.group(1)) if m else 999

def order_images(imgs):
    """Rækkefølge ud fra billed-NUMMERET i filnavnet (ikke Fot-kolonnerækkefølgen):
    billede 3 først, så 1, så 2, derefter resten i stigende orden. Billede 0 har
    intet fortrin og lander dermed lige efter de første tre (3,1,2,0,4,5,…)."""
    rank = {3: 0, 1: 1, 2: 2}
    return sorted(imgs, key=lambda u: rank.get(_img_num(u), 3 + _img_num(u)))

def images_for(row, is_bulb=False):
    imgs = []
    for i in range(1, 21):
        v = row.get(f'Fot_{i}')
        if pd.notna(v) and str(v).strip().startswith('http'):
            imgs.append(str(v).strip())
    # Pærer (Lyskilde): NATURLIG rækkefølge (1,2,3,…) — som eksisterende pærer.
    # Lamper: rotation 3,1,2,… (billede 0 efter de første tre).
    return sorted(imgs, key=_img_num) if is_bulb else order_images(imgs)

def _bulb_specs_html(row):
    """Specs-blok til pærer (deres Long/Short er ofte N/D)."""
    li = []
    for label, col in [('Fatning', 'Trådtype'), ('Effekt', 'Maksimal effekt (W)'),
                       ('Lysstrøm', 'Lumen'), ('Lysfarve', 'Kelvins'), ('Energiklasse', 'Energiklasse')]:
        v = row.get(col)
        if not is_nd(v):
            unit = {'Effekt': ' W', 'Lysstrøm': ' lm', 'Lysfarve': ' K'}.get(label, '')
            li.append(f"<li><strong>{label}:</strong> {str(v).strip()}{unit}</li>")
    return '<ul>' + ''.join(li) + '</ul>' if li else ''

def build_body_html(long_html, short_html, is_bulb=False, row=None):
    """Produkt-body (descriptionHtml): <h4>Beskrivelse>{long} + <h4>ProduktInfo>{short}.
    NB: 'ProduktInfo' med stort I — matcher temaets customtabs-parsing (eksisterende katalog)."""
    parts = []
    if not is_nd(long_html):
        parts.append('<h4>Beskrivelse</h4>'); parts.append(str(long_html).strip())
    if not is_nd(short_html):
        parts.append('<h4>ProduktInfo</h4>'); parts.append(str(short_html).strip())
    elif is_bulb and row is not None:
        specs = _bulb_specs_html(row)
        if specs:
            parts.append('<h4>ProduktInfo</h4>'); parts.append(specs)
    return '\n'.join(parts)

def produktinfo_html(long_html, short_html, is_bulb=False, row=None):
    """Variant-metafelt custom.produktinfo = BÅDE beskrivelse (long benefits) OG specs (short),
    konkateneret UDEN h4 — matcher eksisterende Sollux (verificeret: live == long+short).
    Varianter kan have unikke beskrivelser, så hele indholdet skal med pr. variant."""
    parts = []
    if not is_nd(long_html): parts.append(str(long_html).strip())
    if not is_nd(short_html): parts.append(str(short_html).strip())
    if parts: return '\n'.join(parts)
    if is_bulb and row is not None:
        return _bulb_specs_html(row)
    return ''

def seo_desc_fallback(short_html, title):
    if is_nd(short_html): return title
    t = re.sub(r'<[^>]+>', ' ', str(short_html))
    t = ' '.join(t.split())
    return (t[:157] + '...') if len(t) > 160 else t

# --- K-rum-tags (heuristisk baseline; AI-lag kan tilføjes senere) ---
def build_ktags(ptype):
    if ptype == 'Lyskilde': return []
    if ptype == 'Skrivebordslamper': return ['K4', 'K6', 'K8']
    return ['K2', 'K5', 'K6', 'K7', 'K8']  # kerne-trio K6/K7/K8 + typiske K2/K5

# --- Claude SEO (batch, chunket 25/kald så lange kørsler ikke trunkeres) ---
def generate_seo_batch(items):
    if not ANTHROPIC_API_KEY or not items:
        return {}
    out = {}
    for i in range(0, len(items), 25):
        out.update(_generate_seo_chunk(items[i:i+25]))
        time.sleep(0.3)
    return out

def _generate_seo_chunk(items):
    """items: [{handle,title,ptype,fatning,colors,material,stil,short}]. -> {handle:{title,description}}"""
    if not ANTHROPIC_API_KEY or not items:
        return {}
    sys_p = ("Du skriver dansk SEO for webshoppen BoligRetning (boligindretning/belysning). "
             "For hvert produkt: en SEO-title (45-65 tegn, format '{type} {MODEL} {evt. variant} - {gevinst/materiale/stil} {evt. fatning}', "
             "tilføj ALTID ' | BoligRetning' til sidst; ALDRIG priser) og en meta-description "
             "(140-160 tegn, nævn 2-3 rum + fatning + en design/kvalitets-closer; ALDRIG priser). "
             "Svar KUN med et JSON-array: [{\"handle\":...,\"title\":...,\"description\":...}].")
    lines = [{"handle": it['handle'], "produkt": it['title'], "type": it['ptype'],
              "fatning": it.get('fatning'), "farver": it.get('colors'),
              "materiale": it.get('material'), "stil": it.get('stil')} for it in items]
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 4000, "system": sys_p,
                  "messages": [{"role": "user", "content": json.dumps(lines, ensure_ascii=False)}]}, timeout=120)
        r.raise_for_status()
        txt = r.json()['content'][0]['text']
        m = re.search(r'\[.*\]', txt, re.DOTALL)
        arr = json.loads(m.group(0) if m else txt)
        return {x['handle']: {'title': x['title'][:70], 'description': x['description']} for x in arr}
    except Exception as e:
        print(f"  ⚠ Claude SEO fejlede ({e}) — falder tilbage til template")
        return {}

# === GRAPHQL ===========================================================
def gql(query, variables=None):
    payload = {'query': query}
    if variables: payload['variables'] = variables
    for attempt in range(1, 5):
        r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=120)
        r.raise_for_status()
        d = r.json()
        if 'errors' in d:
            if any('hrottl' in str(e) for e in d['errors']) and attempt < 4:
                time.sleep(2 ** attempt); continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        if d.get('extensions', {}).get('cost', {}).get('throttleStatus', {}).get('currentlyAvailable', 1000) < 200:
            time.sleep(0.5)
        return d
    raise Exception("Max retries exceeded")

PRODUCT_SET = """
mutation productSet($input: ProductSetInput!, $synchronous: Boolean) {
  productSet(input: $input, synchronous: $synchronous) {
    product { id title handle status variants(first: 250) { edges { node { id sku } } } }
    userErrors { field message code }
  }
}"""

PUBLICATIONS_Q = "query { publications(first: 25) { edges { node { id name } } } }"

def loc_gid(): return f"gid://shopify/Location/{LOCATION_ID}"

def variant_input(v):
    ov = [{"optionName": n, "name": val} for n, val in v['option_values']]
    inp = {
        "optionValues": ov if ov else [{"optionName": "Title", "name": "Default Title"}],
        "price": str(v['price']), "sku": v['sku'], "barcode": v['barcode'] or None,
        "inventoryItem": {"cost": str(v['cost']), "tracked": True, "requiresShipping": True,
                          "measurement": {"weight": {"value": v['weight_grams']/1000.0, "unit": "KILOGRAMS"}}},
        "inventoryPolicy": "DENY",
        "inventoryQuantities": [{"locationId": loc_gid(), "name": "available", "quantity": v['inventory_quantity']}],
        "metafields": v['metafields'], "taxable": True,
    }
    if v.get('compare_at'):
        inp["compareAtPrice"] = str(v['compare_at'])
    if v.get('image_url'):
        inp["file"] = {"originalSource": v['image_url'], "contentType": "IMAGE", "alt": f"{v['sku']} variant"}
    return inp

def create_product(spec, synchronous=True):
    if spec['options_definition']:
        ordered = {}
        for v in spec['variants']:
            for n, val in v['option_values']:
                ordered.setdefault(n, [])
                if val not in ordered[n]: ordered[n].append(val)
        product_options = [{"name": o, "values": [{"name": x} for x in ordered[o]]} for o in spec['options_definition']]
    else:
        product_options = [{"name": "Title", "values": [{"name": "Default Title"}]}]
    files, seen = [], set()
    for i, url in enumerate(spec['media_urls']):
        if url in seen: continue
        seen.add(url)
        files.append({"originalSource": url, "contentType": "IMAGE",
                      "alt": f"{spec['title']} - {'Hovedbillede' if i==0 else f'Billede {i+1}'}"})
    for v in spec['variants']:
        if v.get('image_url') and v['image_url'] not in seen:
            seen.add(v['image_url'])
            files.append({"originalSource": v['image_url'], "contentType": "IMAGE", "alt": f"{spec['title']} - Variant {v['sku']}"})
    payload = {
        "title": spec['title'], "descriptionHtml": spec['body_html'], "vendor": "Sollux",
        "productType": spec['product_type'], "tags": spec['tags'], "handle": spec['handle'],
        "status": spec['status'],
        "seo": {"title": spec['seo_title'], "description": spec['seo_description']} if spec['seo_title'] else None,
        "productOptions": product_options, "variants": [variant_input(v) for v in spec['variants']],
        "files": files or None, "metafields": spec.get('product_metafields') or None,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    return gql(PRODUCT_SET, {"input": payload, "synchronous": synchronous})['data']['productSet']

_PUBS = None
PUBLISH_M = "mutation($id:ID!,$input:[PublicationInput!]!){ publishablePublish(id:$id,input:$input){ userErrors{message} } }"
def publish_all(product_id):
    """Publicér produkt til ALLE sales channels."""
    global _PUBS
    if _PUBS is None:
        d = gql(PUBLICATIONS_Q)
        _PUBS = [e['node']['id'] for e in d['data']['publications']['edges']]
    if not _PUBS:
        return []
    r = gql(PUBLISH_M, {"id": product_id, "input": [{"publicationId": p} for p in _PUBS]})
    return r.get('data', {}).get('publishablePublish', {}).get('userErrors') or []

# === FEED → SPECS ======================================================
def load_feed():
    d = pd.read_csv(DUNSKI, sep=';', encoding='utf-8', dtype=str)
    pl = pd.read_excel(PRICELIST, header=12)
    cols = list(pl.columns)
    sym, npp = cols[6], cols[14]   # SYMBOL, NET PURCHASE PRICE (første)
    cost_map = {}
    for _, r in pl.iterrows():
        try: cost_map[str(r[sym]).strip()] = float(r[npp])
        except: pass
    av = pd.read_csv(AVAILABILITY, sep=';', encoding='utf-8', dtype=str)
    stock_map = {str(r['SKU']).strip(): int(float(r['QUANTITY'])) for _, r in av.iterrows() if pd.notna(r.get('QUANTITY'))}
    return d, cost_map, stock_map

def fetch_existing_sollux():
    """Hent eksisterende Sollux-SKU'er + handles LIVE fra Shopify (idempotens)."""
    skus, handles = set(), set()
    q = """query($c:String){ products(first:100, query:"vendor:Sollux", after:$c){
      pageInfo{hasNextPage endCursor}
      edges{node{ handle variants(first:30){edges{node{sku}}}}}}}"""
    cur = None
    while True:
        d = gql(q, {"c": cur})
        pr = d['data']['products']
        for e in pr['edges']:
            handles.add(e['node']['handle'])
            for v in e['node']['variants']['edges']:
                if v['node']['sku']:
                    skus.add(v['node']['sku'].strip())
        if not pr['pageInfo']['hasNextPage']:
            break
        cur = pr['pageInfo']['endCursor']
        time.sleep(0.2)
    return skus, handles

def build_specs(limit, status):
    d, cost_map, stock_map = load_feed()
    existing_skus, handles_used = fetch_existing_sollux()
    print(f"Eksisterende i Shopify: {len(existing_skus)} SKU'er, {len(handles_used)} handles")
    lamp_cfg, bulb_cfg, src = load_sollux_configs()
    print(f"Pris-config: {src} | lamper markup={float(lamp_cfg.get('fixed_markup')):.4f} {lamp_cfg.get('rounding')} disc={lamp_cfg.get('fictive_discounts')} | pærer surcharge={bulb_cfg.get('flat_surcharge')}")

    d['Product name'] = d['Product name'].apply(lambda x: apply_fixes(x, TEXT_FIXES))
    d['_color'] = d['Farve'].apply(norm_color)
    d['_base'] = d.apply(lambda r: base_from_name_color(r['Product name'], r['_color']), axis=1)
    d['_has_cost'] = d['SKU'].apply(lambda s: str(s).strip() in cost_map)

    specs, skipped = [], 0
    for base, grp in d.groupby('_base'):
        skus = [str(s).strip() for s in grp['SKU']]
        if not all(s not in existing_skus for s in skus):
            skipped += 1; continue
        grp = grp[grp['_has_cost']]
        if len(grp) == 0: continue
        prod_row = grp.iloc[0]
        ptype, type_is_new = map_type(prod_row.get('Kategori'))
        is_bulb = ptype == 'Lyskilde'
        is_variant_group = (not is_bulb) and len(grp) > 1 and grp['_color'].notna().any()
        cfg = bulb_cfg if is_bulb else lamp_cfg

        title = str(prod_row['_base'])
        handle = generate_handle(title, handles_used)   # seed til fictive rabat (samme rabat for alle varianter)
        series = extract_series(prod_row['Product name'])
        fatning = fatning_of(prod_row)

        variants, media = [], []
        for pos, (_, row) in enumerate(grp.iterrows()):
            sku = str(row['SKU']).strip()
            gross_eur = float(row['Gross retail price (EUR)'])
            # Pris fra HUB-reglen. Markup-base = RRP-afledt kostpris (gross/1,20×0,55×7,45),
            # så markup 2,1818 reproducerer den oprindelige pris (gross×7,45) 100%.
            pricing_cost = gross_eur / 1.20 * 0.55 * EUR_TO_DKK
            price, compare_at = pricing.resolve_variant_pricing(pricing_cost, cfg, seed=handle)
            # Variant Cost (unitCost) = reel indkøbspris fra prisliste (NPP), som eksisterende.
            cost = int(cost_map[sku] * EUR_TO_DKK)
            try: w = int(float(str(row.get('Nettovægt (kg)', '0')).replace(',', '.')) * 1000)
            except: w = 0
            imgs = images_for(row, is_bulb)
            color = row['_color']
            ov = [("Farve", color)] if (is_variant_group and color) else []
            mf = [{"namespace": "custom", "key": "sku", "type": "single_line_text_field", "value": sku}]
            if pos > 0:
                pi = produktinfo_html(row.get('Long description HTML - benefits'), row.get('Short description HTML'), is_bulb, row)
                if pi: mf.append({"namespace": "custom", "key": "produktinfo", "type": "multi_line_text_field", "value": pi})
                if imgs: mf.append({"namespace": "custom", "key": "variantbilleder", "type": "list.single_line_text_field", "value": json.dumps(imgs)})
            variants.append({"sku": sku, "price": price, "compare_at": compare_at, "cost": cost, "weight_grams": w,
                             "inventory_quantity": stock_map.get(sku, 0), "barcode": str(row.get('EAN') or ''),
                             "option_values": ov, "image_url": imgs[0] if imgs else None, "metafields": mf})
            if pos == 0: media = imgs

        # --- TAGS (reproducér eksisterende \xa0-konvention; tabsmenu plain) ---
        tags = ['tabsmenu', NBSP + 'Sollux', NBSP + ptype]
        if series: tags.append(NBSP + series)
        for _, row in grp.iterrows():           # farve-tags: lowercase rå farve m. nbsp
            rc = raw_color(row.get('Farve'))
            if rc: tags.append(NBSP + rc)
        if fatning: tags.append(NBSP + fatning)
        if str(prod_row.get('TOP', '')).upper() == 'TOP': tags.append(NBSP + 'TOP')
        for kt in build_ktags(ptype): tags.append(NBSP + kt)

        # --- produkt-metafelter: google-kategori + anbefalet_paere (mersalg) ---
        pmf = [{"namespace": "custom", "key": "google_category_id", "type": "single_line_text_field", "value": "594"}]
        if not is_bulb and fatning and fatning in FATNING_TO_BULB:
            pmf.append({"namespace": "custom", "key": "anbefalet_paere", "type": "product_reference",
                        "value": FATNING_TO_BULB[fatning]})

        spec = {
            "handle": handle, "title": title,
            "body_html": build_body_html(prod_row.get('Long description HTML - benefits'),
                                         prod_row.get('Short description HTML'), is_bulb, prod_row),
            "product_type": ptype, "new_type": type_is_new, "status": status,
            "tags": sorted(set(tags)),
            "seo_title": "", "seo_description": "",          # udfyldes af Claude nedenfor
            "_seo_src": {"title": title, "ptype": ptype, "fatning": fatning,
                         "colors": sorted({c for c in grp['_color'] if pd.notna(c)}),
                         "material": None if is_nd(prod_row.get('Materiale')) else str(prod_row.get('Materiale')),
                         "stil": None if is_nd(prod_row.get('Stil')) else str(prod_row.get('Stil')),
                         "short": prod_row.get('Short description HTML')},
            "options_definition": ["Farve"] if is_variant_group else [],
            "media_urls": media, "variants": variants, "product_metafields": pmf,
        }
        specs.append(spec)
        if len(specs) >= limit: break

    # --- SEO i batch via Claude ---
    items = [{"handle": s['handle'], **s['_seo_src']} for s in specs]
    seo = generate_seo_batch(items)
    for s in specs:
        got = seo.get(s['handle'])
        if got:
            s['seo_title'], s['seo_description'] = got['title'], got['description']
        else:
            s['seo_title'] = (s['title'] + ' | BoligRetning')[:70]
            s['seo_description'] = seo_desc_fallback(s['_seo_src']['short'], s['title'])
        s.pop('_seo_src', None)
    print(f"Byggede {len(specs)} produkt-specs. Sprang {skipped} merge-grupper over. SEO: {'Claude' if seo else 'template'}.")
    return specs

# === MAIN ==============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--live', action='store_true')
    ap.add_argument('--limit', type=int, default=10)
    ap.add_argument('--status', default='DRAFT')
    ap.add_argument('--fetch', action='store_true', help='Hent feed-filer live (månedligt job/GHA)')
    args = ap.parse_args()
    if args.fetch:
        global DUNSKI, PRICELIST, AVAILABILITY
        dest = os.environ.get('FEED_DIR') or os.path.join(os.path.dirname(__file__), '..', 'output', 'feed')
        print(f"Henter live feed-filer → {dest}")
        fetch_live_feeds(dest)
        DUNSKI = os.path.join(dest, 'CSV_dunski_EUR_wszystkie.csv')
        PRICELIST = os.path.join(dest, 'SOLLUX_PRICELIST_EUR_EN.xls')
        AVAILABILITY = os.path.join(dest, 'products_availability.csv')
    specs = build_specs(args.limit, args.status)
    print(f"\n{'='*60}\n{'LIVE' if args.live else 'DRY-RUN'} — {len(specs)} produkter (status={args.status})\n{'='*60}")
    results = []
    for s in specs:
        opt = f"Farve×{len(s['variants'])}" if s['options_definition'] else 'single'
        nt = '  [NY TYPE]' if s.get('new_type') else ''
        ap_ = next((m['value'].split('/')[-1] for m in s['product_metafields'] if m['key'] == 'anbefalet_paere'), '—')
        print(f"\n• {s['title']}  [{opt}]{nt}  handle={s['handle']}")
        print(f"    type={s['product_type']} | priser={[v['price'] for v in s['variants']]} | anbef.pære={ap_}")
        print(f"    SEO: {s['seo_title']}")
        print(f"    tags={s['tags']}")
        if args.live:
            try:
                r = create_product(s)
                errs = r.get('userErrors') or []
                if errs: print(f"    ❌ {errs}")
                else:
                    p = r['product']
                    pub = ''
                    if s['status'] == 'ACTIVE':
                        perr = publish_all(p['id'])
                        pub = ' 📡publiceret' if not perr else f' ⚠pub-fejl:{perr}'
                    print(f"    ✅ {p['handle']} ({p['status']}){pub} {p['id']}")
                    results.append({"handle": p['handle'], "id": p['id'], "title": s['title'],
                                    "skus": [v['sku'] for v in s['variants']],
                                    "url": f"https://{SHOPIFY_STORE}/admin/products/{p['id'].split('/')[-1]}"})
            except Exception as e:
                print(f"    ❌ FEJL: {e}")
            time.sleep(0.6)
    if args.live and results:
        out = os.path.join(os.path.dirname(__file__), '..', 'output', 'sollux_created.json')
        os.makedirs(os.path.dirname(out), exist_ok=True)
        json.dump(results, open(out, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
        print(f"\n=== TIL VERIFIKATION ===")
        for r in results:
            print(f"  {r['title']} | SKUs {r['skus']}\n    {r['url']}")

if __name__ == '__main__':
    main()
