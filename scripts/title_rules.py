from __future__ import annotations
"""KANONISKE TITEL-REGLER — porteret 1:1 fra vidaxl-pris-lager/scripts/audit_titles.py (100%-valideret 2026-07-05).
Ren deterministisk logik, ingen I/O. Redigér IKKE her uden at opdatere kilden — de skal holdes i sync."""
"""TITEL-AUDIT v2 (read-only) — deterministisk Lag 1+3, minimal LLM.

Filosofi (Gabriel): brug LLM SÅ LIDT som muligt — kun til oversættelse (engelsk)
og fjernelse af foranstillet SKU. Alt andet gøres deterministisk med Shopify-
variant-data (option-NAVNE afslører hvad der er variant: Farve/Højde/Størrelse).

Lag 1: mekanisk (casing, feed-stavefejl, whitespace, orphan units, entities).
Lag 3: deterministisk rekonstruktion — dangling-X mål (=variant-dimension) droppes,
       option-værdier fjernes med guards (hovedord, og-rester, delvis farve).
Lag 2: embedding-outlier (fra tmp_title_outliers) som LAV-tillid review-flag.
LLM (senere, llm_titles.py): KUN rækker flagget likely_english / leading_sku.

Skriver C:\\Users\\APC\\Desktop\\titel_audit.csv. Rører IKKE Shopify.
"""
import csv, html, json, os, re, sys, time, urllib.request
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

_HUB_ENV = r"C:\Users\APC\Desktop\BR\br-ai-hub\BoligretningAI\.env.local"
if os.path.exists(_HUB_ENV):
    for _l in open(_HUB_ENV, encoding="utf-8"):
        _m = re.match(r"\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$", _l)
        if _m:
            os.environ.setdefault(_m.group(1), _m.group(2).strip().strip('"').strip("'"))


VENDOR = os.environ.get("AUDIT_VENDOR", "vidaXL")
OUT_CSV = os.environ.get("AUDIT_OUT", r"C:\Users\APC\Desktop\titel_audit.csv")
SB = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or "").rstrip("/")
SKEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SEM_THRESHOLD = 0.689  # p95 distance → semantisk outlier

# ============================================================ REGLER
def clean_vidaxl(text):
    if not text: return ""
    for v in ["vidaXL ", "vidaxl ", "VidaXL ", "VIDAXL ", "fra vidaXL", "vidaXL", "vidaxl"]:
        text = text.replace(v, "")
    return text.strip()

def fix_pcs_to_dele(text):
    return re.sub(r"\bpcs\b", "dele", text or "", flags=re.IGNORECASE)

# Feed-fejl-ordbog (systematiske vidaXL-trunkeringer), case-bevarende. UDVIDBAR.
FEED_SPELLING = [
    (re.compile(r"(?i)\bu(træk)"), lambda m: m.group(0)[0] + "dtræk", "utraekkelig"),        # Utrækkelig→Udtrækkelig
    (re.compile(r"(?i)\b(e)træsfarve"), lambda m: m.group(0)[0] + "getræsfarve", "egetraesfarve"),  # Etræsfarve(t)→Egetræsfarve(t)
    (re.compile(r"(?i)\bsofa(esæt)"), lambda m: m.group(0)[0] + "ofasæt", "sofaesaet"),        # Sofaesæt→Sofasæt
]
def fix_feed_spelling(text):
    for pat, fn, _ in FEED_SPELLING:
        text = pat.sub(fn, text)
    return text

UPPER_TOKENS = ["LED","TV","USB","UV","PVC","PE","PP","PET","RGB","HDMI","HD","3D","WC","CD","DVD","MDF","HDPE","WPC","ABS","XXL","XL","WiFi",
                "GGL","GGU","GPL","GPU","GHL","GHU","GVK","VKU"]   # + PE/PP/PET-plast, Velux-koder. SPA fjernet (skal være "Spa")
def fix_casing(text):
    hits = []
    for tok in UPPER_TOKENS:
        def _r(m, t=tok):
            if m.group(0) != t: hits.append(t)
            return t
        text = re.sub(r"\b" + re.escape(tok) + r"\b", _r, text, flags=re.IGNORECASE)
    def _ip(m):
        c = "IP" + m.group(1)
        if m.group(0) != c: hits.append(c)
        return c
    text = re.sub(r"(?i)\bip(\d{2})\b", _ip, text)
    return text, hits

MOJIBAKE = ["Ã", "Â", "â€", "ï¿½", "Ð", "Ñ"]
CONNECT = {"og", "med", "i", "til", "på", "af", "samt", "&", "+"}
# KUN rene farver (IKKE materialer/træsorter — dem beholder vi som en del af navnet).
# Fler-ords-finish (fx 'Grå sonoma-eg', 'Røget eg') fjernes af exact-match mod produktets EGNE
# Farve-værdier — ikke af dette leksikon.
COLOR_LEX = {
    "sort","hvid","hvidt","grå","gråt","graa","brun","brunt","rød","rødt","orange","gul","gult",
    "grøn","grønt","blå","blåt","lilla","violet","pink","lyserød","magenta","turkis","cyan",
    "lysegrå","lysebrun","lyseblå","lysegrøn","mørkegrå","mørkebrun","mørkegrøn","mørkeblå","mørkerød",
    "gyldenbrun","betongrå","gråbrun","gråbeige","antracitgrå","antracit","grafit",
    "beige","creme","sand","khaki","taupe","oliven","olivengrøn","naturfarvet","flerfarvet",
    "transparent","gennemsigtig","klar",
    "bordeaux","vinrød","koral","fersken","laks","terrakotta","rust","cognac","camel","okker","sennep",
    "kaffe","mokka","chokolade","cappuccino","aubergine",
    "sølv","soelv","guld","gylden","guldfarvet","sølvfarvet","bronze","kobber","messing","krom","nikkel",
    "egetræsfarve","egetræsfarvet","mat","blank","højglans","skinnende","marineblå","aqua","mint","petroleum",
    # ekstra farve-former (feed bruger -farvet-endelser + finish-navne)
    "cremefarvet","naturfarvet","gråbrun","gråbeige","lysebrun","lyseblå","lysegrøn","mørkegrøn","mørkeblå",
    "sonoma","sonoma-eg","antikgrå","betonggrå","gyldenbrun","vinrød","aubergine","koral","fersken","laks",
    "cappuccino","mokka","kaffe","chokolade","rustfri","forkromet","kobberfarvet","bronzefarvet","messingfarvet",
    "natur","naturlig","antracit","grafitgrå","stengrå","sølvgrå","perlehvid","råhvid","offwhite","antikbrun",
    # finish-modifikatorer (fanger fler-ords-finish-rester som 'Røget' i 'Røget Sonoma-Eg')
    "røget","poleret","børstet","antik","patineret","rustik","lakeret","oljet","vintage",
}
COLOR_OPTION_NAMES = {"farve", "color", "colour", "farbe", "kulør"}
DIM_OPTION_NAMES = {"højde","hojde","længde","laengde","bredde","dybde","diameter","størrelse","stoerrelse",
                    "mål","maal","tykkelse","size","height","width","length","depth"}
STICKY = ["stk","sæt","dele","pak","pakke","par"]
ENGLISH_MARKERS = {"with","and","the","black","white","grey","gray","mirror","cabinet","chair","garden",
                   "wall","wood","steel","bathroom","kitchen","storage","folding","outdoor","cushion",
                   "frame","bench","shelf","stool","door","drawer","piece","pieces","brown","blue","green",
                   "cover","seat","table","drawers","clock","rack","desk","stand"}

def _looks_english(t):
    words = re.findall(r"[A-Za-z]+", (t or "").lower())
    return sum(1 for w in words if w in ENGLISH_MARKERS) >= 2

def is_dim_option(name):
    n = (name or "").lower().strip()
    return any(d in n for d in DIM_OPTION_NAMES)

# dinglende mål: tal(-x-tal)* der ender på et X uden efterfølgende tal/paren/komma → variant-dimension.
# (komma i lookahead → rør ikke garblede mål som '112,5X51X,5X96,5')
DANGLING_DIM = re.compile(r"\b\d+(?:[.,]\d+)?(?:\s*[xX]\s*\d+(?:[.,]\d+)?)*\s*[xX](?!\s*[\d(,])")

def cleanup_connectives(s):
    toks = s.split()
    while toks and toks[-1].lower().strip(".,-–") in CONNECT: toks.pop()
    while toks and toks[0].lower().strip(".,-–") in CONNECT: toks.pop(0)
    out = []
    for w in toks:
        if out and w.lower().strip(".,-–") in CONNECT and out[-1].lower().strip(".,-–") in CONNECT:
            continue
        out.append(w)
    return " ".join(out)

def has_color_option(opts_by_name):
    return any((name or "").lower().strip() in COLOR_OPTION_NAMES for name in opts_by_name)

def strip_trailing_color_phrase(s):
    """Fjern den bagerste sammenhængende RENE farve-frase (kun COLOR_LEX + 'og'/'/'), bevar mindst
    1 indholdsord. Materialer/træsorter er IKKE i leksikonet → bevares. Bruges kun når produktet
    har en Farve-option (farve = variant)."""
    toks = s.split()
    while len(toks) > 1:
        w = toks[-1].lower().strip(".,-–/&")
        if w in COLOR_LEX or w in CONNECT or w in {"/", "&"}:
            toks.pop()
        else:
            break
    return " ".join(toks)

def _deplural(w):
    w = (w or "").lower().strip()
    for suf in ("erne", "ene", "er", "e", "r"):
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            return w[:-len(suf)]
    return w

def type_stems(ptype):
    return {_deplural(w) for w in re.findall(r"[A-Za-zÆØÅæøå]+", ptype or "") if len(w) >= 3}

def is_type_word(val, stems):
    v = _deplural(val)
    return any(s == v or s.startswith(v) or v.startswith(s) for s in stems if len(s) >= 3)

# ægte ordgrænse: IKKE limet til bindestreg, bogstav eller ciffer (undgår kompound-brud)
def _boundpat(inner):
    return re.compile(r"(?<![-\wæøåÆØÅ])" + inner + r"(?![-\wæøåÆØÅ])", re.IGNORECASE)

def strip_options_guarded(s, opts_by_name, vc, ptype=""):
    """Fjern farve/materiale/antal-option-værdier med guards. Dim-options håndteres separat.
    Guards: (1) type-ord (=kategori) fjernes aldrig; (2) match kun ægte ordgrænser (ikke
    limet til bindestreg → 'Chesterfield-Sofa' brydes ikke); (3) hovedord (første ord) skånes."""
    removed, labels, flags = [], set(), set()
    stems = type_stems(ptype)
    vals = []
    for name, values in opts_by_name.items():
        if is_dim_option(name):
            continue  # mål håndteres via dangling-drop, ikke string-match
        for v in values:
            vals.append(v)
    vals = sorted(set(vals), key=lambda x: (-len(x.split()), -len(x)))
    for v in vals:
        vv = str(v).strip()
        if not vv:
            continue
        if vv.isdigit():
            if vc <= 1:
                continue
            sp = "|".join(STICKY)
            pat = _boundpat(re.escape(vv) + r"\s+(?:" + sp + r")\.?")
            if pat.search(s):
                s = pat.sub(" ", s); removed.append(f"{vv} (antal)"); labels.add("count_strip")
            continue
        if is_type_word(vv, stems):  # GUARD 1: type-ord (Sofa i kat. Sofaer) skånes
            flags.add("type_word_protected"); continue
        pat = _boundpat(re.escape(vv))  # GUARD 2: kun ægte ordgrænse (ikke kompound)
        m = pat.search(s)
        if m:
            if m.start() == 0:  # GUARD 3: hovedord (første ord) skånes
                flags.add("head_noun_review"); continue
            s = pat.sub(" ", s); removed.append(vv); labels.add("leftover_option")
        else:
            og = re.match(r"^(.+?)\s+og\s+(.+)$", vv, re.IGNORECASE)
            if og:
                pre = og.group(1).strip()
                fz = _boundpat(re.escape(pre) + r"\s+og\s+\S+")
                if fz.search(s):
                    s = fz.sub(" ", s); removed.append(vv); labels.add("leftover_option")
    if has_color_option(opts_by_name):  # farve ER variant → fjern bagerste RENE farve-frase
        s2 = strip_trailing_color_phrase(s)
        if s2 != s:
            s = s2; labels.add("trailing_color")
    return s, removed, sorted(labels), sorted(flags)

def analyze(title, opts_by_name, vc, ptype=""):
    t = title or ""
    issues, removed = [], []
    needs_llm = ""

    s = html.unescape(t)
    if s != t: issues.append("html_entity")
    if any(m in t for m in MOJIBAKE): issues.append("mojibake_review")
    b = re.sub("[   -​  ⁠　﻿	]+", " ", s)  # usynligt NBSP/unicode-mellemrum -> normalt
    if b != s: issues.append("whitespace"); s = b

    # LLM-triggere: KUN engelsk (oversæt) + foranstillet SKU (fjern tal)
    if re.match(r"\s*\d{4,}\b", t):
        issues.append("leading_sku"); needs_llm = "sku"
    if _looks_english(t):
        issues.append("likely_english"); needs_llm = "english"

    orig = s
    s = fix_feed_spelling(s)
    if s != orig:
        for pat, fn, lbl in FEED_SPELLING:
            if pat.search(orig): issues.append(lbl)

    b = clean_vidaxl(s)
    if b != s: issues.append("vidaxl_token")
    s = b
    b = fix_pcs_to_dele(s)
    if b != s: issues.append("pcs")
    s = b

    # ---- Lag 3: deterministisk rekonstruktion (engelsk overlades til LLM) ----
    if needs_llm != "english":
        nb = DANGLING_DIM.sub(" ", s)
        if nb != s:
            issues.append("dim_dropped"); s = nb  # variant-dimension → drop hele blokken
        s, rem, opt_labels, opt_flags = strip_options_guarded(s, opts_by_name, vc, ptype)
        removed += rem; issues += opt_labels; issues += opt_flags

    b = re.sub(r"(?<!\d)\s+[xX]\s+(?!\d)", " ", s)
    if b != s: issues.append("orphan_x"); s = b
    b = re.sub(r"(?<!\d)\s+[Cc][Mm]\.?\b", "", s); b = re.sub(r"(?<!\d)\s+[Mm][Mm]\.?\b", "", b)
    if b != s: issues.append("orphan_unit"); s = b
    b = re.sub(r"(?<!\d),(?!\d)", "", s)
    if b != s: issues.append("stray_comma"); s = b
    b = re.sub(r"(?<=\S)-(?=\s)", "", s)  # hængende bindestreg: 'Chesterfield- Stof' → 'Chesterfield Stof'
    b = re.sub(r"(?<=\s)-(?=\S)", "", b)
    if b != s: issues.append("hyphen_cleanup"); s = b
    b = cleanup_connectives(s)
    if b != s: issues.append("connective_cleanup"); s = b
    b = re.sub(r"\s+", " ", s).strip(" ,-–")
    if b != s: issues.append("whitespace"); s = b

    s, casing_hits = fix_casing(s)
    if casing_hits: issues.append("casing:" + "/".join(sorted(set(casing_hits))))

    suggested = s.strip()
    content = [w for w in suggested.split() if w.lower().strip(".,-–") not in CONNECT]
    if len(suggested) < 3 or len(content) < 1:
        issues.append("over_stripped_review"); suggested = ""

    changed = bool(suggested) and suggested != title
    return suggested, changed, issues, removed, needs_llm

# ============================================================ EXPORT
_BULK_RUN = "mutation($q:String!){bulkOperationRunQuery(query:$q){bulkOperation{id status} userErrors{message}}}"
_BULK_STAT = "query{currentBulkOperation(type:QUERY){id status errorCode objectCount url}}"

