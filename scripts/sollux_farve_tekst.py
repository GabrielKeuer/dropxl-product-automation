"""Fjern farve fra titel, SEO-titel og beskrivelse på Sollux-produkter med FLERE farver.

Regel (Omar 11/11-2026): farven ligger i variantvælgeren, så tekster nævner den ikke.
 - Beskrivelse: 'Farve:'-speclinjer og sætninger om farver fjernes; "i olivengrøn" strippes
   fra ellers neutrale sætninger; henvisninger ("Dette gør det nemt...") efter en fjernet
   farvesætning fjernes / "Begge varianter er" -> "Lampen er". Lysfarve (varm hvid, 3000K),
   pære-sætninger og materialer (træ/beton) bevares.
 - Titel: hale efter sidste MODEL-ord/tal fjernes, hvis den indeholder en farve.
 - SEO-titel: farveord og "i 3 farver" fjernes.
Idempotent: kørt på allerede rensede tekster ændres intet. Port af scripts/tmp-regel brugt
live 11/11 (paritet testet mod 256 produkter).

Brug:
  python scripts/sollux_farve_tekst.py            # dry-run: vis hvad der ville ændres
  python scripts/sollux_farve_tekst.py --apply    # skriv til Shopify
Env: SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN
"""
import os, re, sys, json, time, argparse

W = r'[A-Za-z0-9_]'
B = rf'(?:(?<={W})(?!{W})|(?<!{W})(?={W}))'   # JS-\b (ASCII) så resultatet matcher JS-reglen 1:1
I = re.I

FARVE = re.compile(r'(^|[^a-zæøå])(farve(n|r|rne|valg|mulighed|muligheder|kombination|kombinationer|udvalg|varianter?|nuancer?)?|farvede?|farvet|farverig[et]?|flerfarvet|nuancer?n?e?|sort|sorte|mat-?sort|matsort|hvid|hvide|grå|olivengrøn(ne)?|oliven|beige|taupe|bordeaux|ultramarin|lyserød[et]?|salvie(grøn)?|okk(er)?(rød[et]?)?|rød[et]?|rødlig|gul[et]?|brun[et]?|lyseblå|blå|marineblå|grøn(ne)?|antracit(farvede?)?|guld|gylden|gyldne|guldfarvede?|krom|kromfarvede?|forkromede?|messing|kobber|sølv|sølvfarvede?|orange|pastel(farver?)?|pink|lilla)(?![a-zæøå])', I)
LYS = re.compile(rf'(varm|neutral|kold)\s?hvid|{B}pære|farvetemperatur|lysfarve|kelvin|\d\s?k{B}|hvidt lys|hvide lys|varmhvid|kold ?hvid|dagslys|ra\s?[>≥]|cri', I)
LYS_BEHOLD = re.compile(r'(^|[^a-zæøå])(pære|pærer|lyskilde|lyskilder|lysets|lysintensitet|CRI|gengiver|lysstyrke)([^a-zæøå]|$)|(skarp|varm|kølig|kold|neutral) hvid|gule nuancer|matte? skærm|mat hvid skærm', I)
FARVEORD_REN = re.compile(r'(^|[^a-zæøå])farve(n|r|rne|valg|muligheder)?([^a-zæøå]|$)', I)
STAERK = re.compile(r'(sort|grå|olivengrøn|beige|taupe|guld|krom|messing|antracit)', I)
C = r'(?:mat ?sort|sort|hvid|grå|olivengrøn|beige|taupe|bordeaux|ultramarin|lyserød|salvie|salviegrøn|okker|rød okker|okkerrød|okkrød|antracit|guld|gylden|krom|messing|kobber|sølv|lyseblå|marineblå|gul|rød|brun|orange)'
FRASE = re.compile(r'\s+i\s+(?:farven\s+|den\s+' + W + r'+\s+farve\s+)?' + C + r'(?:\s*(?:\/|og|eller|,)\s*' + C + r')*(?:\s+(?:finish|udgave|udførelse|farve))?(?=\s*[,.!?:;]|\s*$|\s+(?:er|der|som|med|til|kombinerer|skaber|giver|tilføjer|bringer|passer|fra)' + B + ')', I)
FAAS_OG = re.compile(r'\s+(?:fås|findes)\s+i\s+' + C + r'(?:\s*(?:\/|og|eller|,)\s*' + C + r')*\s+og\s+', I)
FAAS = re.compile(rf'{B}(fås|findes|tilgængelig){B}', I)
SPEC = re.compile(r'<(li|p)[^>]*>\s*(<(strong|b)>)?\s*(farve|farver|farvevalg|farvemuligheder|farvevarianter|farvekombination|finish|farve\/finish)\s*:?\s*(<\/(strong|b)>)?\s*:?[^<]*(<[^>]+>[^<]*)*?<\/\1>', I)
SPLIT = re.compile(r'(?<=[.!?])\s+(?=[A-ZÆØÅ0-9])')
TAG = re.compile(r'<[^>]+>')
BEGGE = re.compile(rf'^Begge (varianter|versioner|udgaver) (er|har){B}')
HENVIS = re.compile(rf'^(Dette|Disse|Begge|Det giver|Det gør|Den gør|Uanset hvilken (farve|finish|variant|version)){B}')

def _plain(s): return TAG.sub(' ', s)

def rens_beskrivelse(html):
    h = html or ''
    state = {'slettet': False}

    def spec(m):
        x = m.group(0)
        if re.search(r'lysfarve|farvetemp', x, I): return x
        if re.search(r'<(strong|b)>\s*finish', x, I) and not FARVE.search(re.sub(r'finish', '', _plain(x), flags=I)): return x
        if not re.search(r'<(strong|b)>\s*(farve|farver|farvevalg|farvemuligheder|farvevarianter|farvekombination|finish)', x, I) \
           and not re.search(r'^<(li|p)[^>]*>\s*(farve|farver)\s*:', x, I): return x
        return ''
    h = SPEC.sub(spec, h)
    h = re.sub(r'<(h[1-6])[^>]*>[^<]*farve[^<]*<\/\1>', '', h, flags=I)

    def afsnit(m):
        tag, attr, inner = m.group(1), m.group(2), m.group(3)
        if re.search(r'<(p|li|ul|ol|h\d)', inner, I): return m.group(0)
        if not FARVE.search(_plain(inner)): return m.group(0)
        keep = []
        for se in SPLIT.split(inner):
            pl0 = _plain(se).strip()
            if re.search(r'mælkeagtige', pl0, I):
                keep.append(se); state['slettet'] = False; continue
            if state['slettet']:
                if BEGGE.search(pl0):
                    se = re.sub(r'Begge (varianter|versioner|udgaver) (er|har)', r'Lampen \2', se, count=1)
                    for a, b in [(rf'{B}berigede{B}', 'beriget'), ('Lampen er nemme', 'Lampen er nem'),
                                 ('Lampen er alsidige og lette', 'Lampen er alsidig og let'), ('Lampen har den samme', 'Lampen har en'),
                                 ('Lampen har en smukke geometriske', 'Lampen har en smuk geometrisk'),
                                 ('og vil forblive relevante', 'og vil forblive relevant')]:
                        se = re.sub(a, b, se, count=1)
                elif HENVIS.search(pl0):
                    state['slettet'] = True; continue
            strip = FAAS_OG.sub(' ', se)
            strip = se if FAAS.search(strip) else FRASE.sub('', strip)
            if strip != se and not FARVE.search(_plain(strip)):
                keep.append(strip); state['slettet'] = False; continue
            plain = _plain(se)
            if LYS_BEHOLD.search(plain):
                keep.append(re.sub('denne sort lysekrone', 'denne lysekrone', se, flags=I)); state['slettet'] = False; continue
            lys_undtag = LYS.search(plain) and not FARVEORD_REN.search(re.sub('lysfarve|farvetemperatur', '', plain, flags=I)) \
                         and not STAERK.search(plain)
            if FARVE.search(plain) and not lys_undtag:
                state['slettet'] = True
            else:
                keep.append(se); state['slettet'] = False
        if not keep: return ''
        return f'<{tag}{attr}>{" ".join(keep)}</{tag}>'
    h = re.sub(r'<(p|li)([^>]*)>((?:(?!<\/\1>)[\s\S])*)<\/\1>', afsnit, h, flags=I)
    h = h.replace(' i den smukke okkerfarve', '')
    h = re.sub(r'<ul[^>]*>\s*<\/ul>', '', h, flags=I)
    h = re.sub(r'\n{3,}', '\n\n', h)
    return h

TITEL_FARVE = re.compile(r'(^|[^a-zæøå])(' + C[3:-1] + r'|naturtræ|naturligt træ|træ|beton|marmor)(?![a-zæøå])', I)

def rens_titel(title):
    """Fjern hale efter sidste MODEL-ord (STORE bogstaver) / tal, hvis halen er en farve."""
    words = str(title).strip().split(); last = -1
    for i, w in enumerate(words):
        wc = w.strip('.,/')
        if (len(wc) >= 2 and any(c.isupper() for c in wc) and not any(c.islower() for c in wc)) or re.fullmatch(r'\d+([.,]\d+)?[A-Z]*', wc):
            last = i
    if last < 0 or last == len(words) - 1: return title.strip()
    tail = ' '.join(words[last + 1:])
    return ' '.join(words[:last + 1]) if TITEL_FARVE.search(tail) else title.strip()

SEO_FARVE = C
def rens_seo_titel(seo, variant_farver=()):
    """Fjern farveord, farvekombinationer og 'i N/flere farver' fra SEO-titel.
    Træ/beton/marmor fjernes kun hvis det er farvevalg (findes i NOGLE men ikke alle varianter);
    ellers er det materialet og bliver stående. Uændret titel returneres uændret."""
    if not seo: return seo
    orig = s = seo.strip()
    vl = [v.lower() for v in variant_farver]
    mat = []
    for stamme, ord_ in (('træ', ['naturligt træ', 'naturtræ', 'træ']), ('beton', ['beton']), ('marmor', ['marmor'])):
        if any(stamme in v for v in vl) and not all(stamme in v for v in vl): mat += ord_
    cc = '(?:' + '|'.join(mat + [C[3:-1]]) + ')'
    combo = cc + r'(?:\s*(?:\/|&|og|,)\s*' + cc + r')*'
    MAT = r'(?=(?:træ|beton|marmor|metal|stål|glas)(?![a-zæøå]))'
    s = re.sub(r'\s+i\s+(?:\d+|flere|mange)\s+farver', '', s, flags=I)
    s = re.sub(r'\s*-?\s*(?:\d+|flere|mange)\s+farver', '', s, flags=I)
    s = re.sub(r'\s+i\s+' + combo + r'\s*(?:\/|&)\s*' + MAT, ' med ', s, flags=I)
    s = re.sub(r'(?<=- )' + combo + r'\s*(?:\/|&)?\s*' + MAT, 'Med ', s, flags=I)
    s = re.sub(r'\s+i\s+' + combo + r'(?![a-zæøå])', '', s, flags=I)
    s = re.sub(r'(?<![a-zæøå])' + combo + r'(?![a-zæøå])\s*', '', s, flags=I)
    if s == orig: return orig
    s = re.sub(r'\s+-\s*(?=\||$)', ' ', s)
    s = re.sub(r'\s+-\s+-\s+', ' - ', s)
    s = re.sub(r'\s+(med|i)\s*(?=\||$)', ' ', s)
    s = re.sub(r'\s{2,}', ' ', s).strip(' -')
    if '|' not in s and len(s) <= 55: s = s + ' | BoligRetning'
    return s

# ---------------- Shopify ----------------
def _gql(q, v=None):
    import requests
    shop, tok = os.environ['SHOPIFY_STORE'], os.environ['SHOPIFY_ACCESS_TOKEN']
    for i in range(5):
        try:
            r = requests.post(f"https://{shop}/admin/api/2024-10/graphql.json", json={"query": q, "variables": v or {}},
                              headers={"X-Shopify-Access-Token": tok, "Content-Type": "application/json"}, timeout=60).json()
            if 'THROTTLED' in json.dumps(r.get('errors', '')): time.sleep(2); continue
            return r
        except Exception:
            time.sleep(3)
    raise RuntimeError('Shopify svarede ikke')

def hent_sollux(handles=None):
    q = """query($c:String,$q:String){products(first:100,query:$q,after:$c){pageInfo{hasNextPage endCursor}
      nodes{id handle title descriptionHtml seo{title} options{name values}}}}"""
    out, cur = [], None
    while True:
        d = _gql(q, {"c": cur, "q": "vendor:Sollux"})['data']['products']
        out += d['nodes']
        if not d['pageInfo']['hasNextPage']: break
        cur = d['pageInfo']['endCursor']; time.sleep(0.2)
    if handles: out = [p for p in out if p['handle'] in set(handles)]
    return out

def plan_for(p):
    farve = next((o for o in p['options'] if o['name'] == 'Farve'), None)
    if not farve or len(farve['values']) < 2: return None
    inp = {}
    nd = rens_beskrivelse(p['descriptionHtml'])
    if nd != (p['descriptionHtml'] or ''): inp['descriptionHtml'] = nd
    nt = rens_titel(p['title'])
    if nt != p['title'].strip(): inp['title'] = nt
    st = (p.get('seo') or {}).get('title')
    ns = rens_seo_titel(st, farve['values']) if st else None
    if st and ns != st.strip(): inp['seo'] = {'title': ns}
    return inp or None

def run(apply=False, handles=None):
    prods = hent_sollux(handles); n = fejl = 0
    M = "mutation($i:ProductInput!){productUpdate(input:$i){userErrors{field message}}}"
    for p in prods:
        inp = plan_for(p)
        if not inp: continue
        n += 1
        print(f"  {p['handle']}: {', '.join(inp)}" + (f" | titel → {inp['title']!r}" if 'title' in inp else '')
              + (f" | SEO → {inp['seo']['title']!r}" if 'seo' in inp else ''))
        if apply:
            e = _gql(M, {"i": {"id": p['id'], **inp}})['data']['productUpdate']['userErrors']
            if e: fejl += 1; print(f"    FEJL {e}")
            time.sleep(0.3)
    print(f"Sollux farvetekst: {n} produkter {'opdateret' if apply else 'ville blive ændret (dry-run)'}, fejl {fejl}")
    return fejl

if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--apply', action='store_true'); ap.add_argument('--handles', nargs='*')
    a = ap.parse_args()
    sys.exit(1 if run(a.apply, a.handles) else 0)
