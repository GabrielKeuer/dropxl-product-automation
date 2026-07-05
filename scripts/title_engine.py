"""KANONISK TITEL-MOTOR — den fulde beviste pipeline fra merge-simulationen (100%-valideret 2026-07-05).

Lag: title_rules.analyze (guards, options-strip, feed-stavefejl) → mål-politik → farve-polish → oprydning/casing.
Politikker (låst i valideringen, ændr ikke uden ny fuld dom):
  - Delt titel (shared=True): variant-akse-egenskaber SKAL ud. Varierer én dimension → HELE målblokken ud (delmål forbudt).
  - Single (shared=False): farve/størrelse ER identitet — kun rens (garble/casing), intet akse-strip.
  - Husets stil: intet 'i/af' før materiale; 'Konstrueret Træ' korrekt; 'N Dele' i sæt-navne er identitet.
Titel-orakel: godkendte titler for eksisterende SKUs ligger i Supabase `vidaxl_approved_titles` (opslag pr. SKU).
Motoren bruges til NYE SKUs/grupper + som fallback. needs_llm ('english'/'sku') → oversæt via LLM som sidste lag.
"""
from __future__ import annotations
import re
from title_rules import analyze

# ---------- casing ----------
_UP = ["LED", "TV", "USB", "UV", "PVC", "RGB", "HDMI", "HD", "3D", "WC", "CD", "DVD", "MDF", "HDPE",
       "WPC", "ABS", "XXL", "XL", "SPA", "WiFi", "PP", "PE", "PU", "EVA", "PET"]

def _cap(w):
    return w[0].upper() + w[1:].lower() if w else w

def _casew(w):  # bindestregs- OG skråstregs-bevidst ('2-personers'→'2-Personers', 'sand/vand'→'Sand/Vand')
    return "/".join("-".join(_cap(p) for p in seg.split("-")) for seg in w.split("/"))

def final_case(t):
    t = " ".join(_casew(w) for w in (t or "").split())
    for tok in _UP:
        t = re.sub(r"\b" + re.escape(tok) + r"\b", tok, t, flags=re.IGNORECASE)
    t = re.sub(r"(?i)\bip(\d{2})\b", lambda m: "IP" + m.group(1), t)
    t = re.sub(r"\((\w)", lambda m: "(" + m.group(1).upper(), t)  # '(stel' → '(Stel'
    return t

def titlecase_feed(t):
    """Feed-titler er lowercase → husets Title Case (før analyze)."""
    for v in ["vidaXL ", "vidaxl ", "VidaXL ", "VIDAXL ", "fra vidaXL", "vidaXL", "vidaxl"]:
        t = (t or "").replace(v, "")
    return " ".join(_casew(w) for w in t.strip().split())

# ---------- mål-politik ----------
_DIM_OPT = {"størrelse", "stoerrelse", "længde", "laengde", "bredde", "dybde", "højde", "hojde",
            "diameter", "mål", "maal", "size", "tykkelse"}
_NUM = r"(?:\d+(?:[.,]\d+)?|\([\d.,\s\-–]+\))"
_COMPLETE_DIM = re.compile(r"(?i)(?<![A-Za-zÆØÅæøå])[øØ⌀]?" + _NUM + r"(?:\s*[xX×]\s*" + _NUM + r")+\s*(?:cm|mm|m)?")
_SINGLE_DIM = re.compile(r"(?i)(?<![A-Za-zÆØÅæøå0-9\-])[øØ⌀]?\d+(?:[.,]\d+)?\s*(?:cm|mm)\b(?!\s*[-–])")
_MALFORMED_DIM = re.compile(r"(?i)(?<![A-Za-zÆØÅæøå])\d[\dxX×,.\s]*\d(?:\s*(?:cm|mm|m))?\b")

def has_dim_axis(options):
    for n, vals in options.items():
        if n.lower().strip() in _DIM_OPT:
            return True
        if any(re.search(r"\d\s*[x×]\s*\d|\b\d+\s*cm\b", v) for v in vals):
            return True
    return False

def strip_dims(t):
    """POLITIK: varierer én dimension → hele målblokken ud (delmål er tvetydige)."""
    t = _COMPLETE_DIM.sub(" ", t)
    t = _SINGLE_DIM.sub(" ", t)
    t = re.sub(r"(?<!\d)\s+(cm|mm|m)\b", "", t, flags=re.I)
    return re.sub(r"\s+", " ", t).strip(" ,-–")

def strip_malformed_dim(t):
    """Drop defekt målblok (x rører komma / dobbelt-x) — vidaXL-garble som '51X,5'."""
    def repl(m):
        b = m.group(0)
        return " " if re.search(r"[xX×]\s*,|,\s*[xX×]|[xX×]\s*[xX×]", b) else b
    return _MALFORMED_DIM.sub(repl, t)

# ---------- farve-polish ----------
FARVE_AXES = ("farve", "color", "colour", "kulør", "hyndefarve", "stelfarve", "rammefarve", "betrækfarve")
COLORS = {  # kun entydige farveord — tvetydige (lys/sand/rust/naturlig) fanges via -farvet-suffiks
    "sort", "sorte", "hvid", "hvidt", "hvide", "grå", "gråt", "gråbrun", "brun", "brunt", "brune",
    "rød", "rødt", "røde", "orange", "gul", "gult", "gule", "grøn", "grønt", "grønne", "blå", "blåt",
    "lilla", "violet", "pink", "lyserød", "turkis", "beige", "creme", "cremehvid", "cremehvide",
    "antracit", "antracitgrå", "taupe", "oliven", "olivengrøn", "bordeaux", "vinrød", "koral",
    "marineblå", "sølv", "guld", "gylden", "bronze", "kobber", "messing", "krom", "nikkel",
    "transparent", "klar", "meleret", "flerfarvet", "aubergine", "cappuccino", "mokka", "terracotta",
    "røget", "antikgrå", "stengrå", "betongrå", "grafitgrå", "sølvgrå", "perlehvid", "råhvid",
    "offwhite", "anthracit",
}
_LIGHTDARK = re.compile(r"(?i)^(lyse?|mørke?|antik|mat|blank)(grå|brun|blå|grøn|rød|beige|gul|lilla|rosa|violet|natur)$")
_CONN = {"og", "med", "i", "på", "af", "samt", "for", "uden", "til", "+", "&", "x"}
_TRAIL_REST = {"farve", "farvet", "lys", "lyse", "mørk", "mørke", "eg", "sonoma", "sonoma-eg", "røget",
               "naturlig", "naturfarvet", "natur"}

def has_color_axis(options):
    return any((n or "").lower().strip() in FARVE_AXES for n in options)

# Antal/panel/kapacitet-akser: værdien i feed-titlen kan stå som 'N-panels', 'N dele',
# 'N stk.', 'N rum', "N LED'er" — men Shopify-optionens værdi kan være bare 'N'. Strip
# robust over bindestreg/mellemrum. GATED: kun tal der ER en variant-akse-værdi.
_COUNT_NOUN = (r"(?:dele|dels|stk\.?|paneler|panels?|rum|personer|pers\.?|sæts?|sæt|niveauer|"
               r"hylder|skuffer|led'?er|lys|låger|døre?|dørs|moduler|sektioner|kurve|kroge?)")
def strip_numeric_axes(t, options):
    for name, vals in (options or {}).items():
        # rammer kun 'tal + tælle-ord' (aldrig rigtige mål som '40x30 cm') → sikkert også på dim-akser
        nums = {m.group(1) for v in vals if (m := re.match(r"\s*(\d+)\b", str(v)))}
        for num in sorted(nums, key=len, reverse=True):
            t = re.sub(rf"(?<![\d.,x×])\b{num}[\s\-]?{_COUNT_NOUN}\b", " ", t, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", t).strip(" ,-–")

def is_color(w):
    lw = w.lower().strip(".,-–+&/")
    if not lw:
        return False
    if lw in COLORS:
        return True
    if re.search(r"farvet?$", lw) and lw != "ufarvet":
        return True
    return bool(_LIGHTDARK.match(lw))

def strip_colors(t, extra_colors=()):
    """Fjern farveord fra delt titel. Beskytter lysfarve-temperatur ('Varmt Hvidt Lys')."""
    extra = {c.lower() for c in extra_colors if c}
    toks = t.split()
    out = []
    for i, w in enumerate(toks):
        lw = w.lower().strip(".,-–")
        nxt = toks[i + 1].lower().strip(".,-–") if i + 1 < len(toks) else ""
        prv = toks[i - 1].lower().strip(".,-–") if i > 0 else ""
        if (is_color(w) or lw in extra) and nxt != "lys" and prv not in ("varm", "varmt", "kold", "koldt"):
            continue
        if lw in ("lys", "mørk", "lyse", "mørke") and nxt not in ("farvet", "farve", "lys") \
                and i + 1 < len(toks) and is_color(toks[i + 1]):
            continue  # 'Lys Grå' → begge ud
        if lw in ("eg", "sonoma", "sonoma-eg") and prv not in ("massiv", "massivt"):
            continue  # dekor-farver på konstrueret træ
        out.append(w)
    st = " ".join(out)
    # guard: strip ikke hvis intet rigtigt navneord (≥3 bogstaver) er tilbage
    mean = [w for w in st.split() if w.lower().strip(".,-–") not in _CONN]
    if not mean or not any(len(re.sub(r"[^A-Za-zÆØÅæøå]", "", w)) >= 3 for w in mean):
        return t
    # trailing rest-ord — men bevar 'Varmt Hvidt Lys'
    toks = st.split()
    while len(toks) > 1 and toks[-1].lower().strip(".,-–") in (_TRAIL_REST | _CONN):
        if toks[-1].lower().strip(".,-–") == "lys" and \
                toks[-2].lower().strip(".,-–") in ("hvid", "hvidt", "varm", "varmt", "kold", "koldt"):
            break
        toks.pop()
    return " ".join(toks)

# ---------- oprydning ----------
def dedup_words(t):  # KUN bogstavsord — aldrig tal/mål
    t = re.sub(r"\b([A-Za-zÆØÅæøå]+\s+[A-Za-zÆØÅæøå]+)\s+\1\b", r"\1", t, flags=re.I)
    t = re.sub(r"\b([A-Za-zÆØÅæøå]{2,})\s+\1\b", r"\1", t, flags=re.I)
    t = re.sub(r"\b([A-Za-zÆØÅæøå]{3,})\s+(?:Og|Med)\s+\1\b", r"\1", t, flags=re.I)
    return t

def cleanup(t):
    t = re.sub(r"^([A-ZÆØÅ][a-zæøå]+)\.(?=\s)", r"\1", t)  # 'Markise. LED' → 'Markise LED'
    t = re.sub(r"(?i)\bog\s+(?=\d+(?:[.,]\d+)?\s*(?:[xX×]|cm|mm|m\b))", "", t)  # dangling og før mål
    t = re.sub(r"(?i)\b(\d+\s+dele|\d+\s+stk\.?|sæt)\s+og\s+(?=(polyrattan|rattan|stof|stål|træ|metal|fløjl|velour|kunstlæder|polyester|aluminium|glas|bambus|jern|plastik|akryl)\b[^ ]*$)", r"\1 ", t)  # '5 Dele Og Polyrattan' (efter strippet farve)
    t = re.sub(r"(x?\(\d+-\d+\))(?!\s*(?:cm|mm|m)\b)(?!\s*[xX×]?\s*[\d(])", r"\1 Cm", t, flags=re.I)  # range-mål mistede enhed (kun i slutning af kæden)
    if "stofbredde" in t.lower() and not re.search(r"(?i)stofbredde\s*:?\s*\d", t):
        t = re.sub(r"(?i)\bstofbredde\b", " ", t)  # orphan label uden tal
    t = re.sub(r"\s*/\s*(?=\s|$)", " ", t)  # orphan slash
    t = re.sub(r"(?i)\s+(?:i|af)\s+(?=(?:massivt?|konstrueret|rustfri|rustfrit|hærdet|forkromet|galvaniseret|pulverlakeret)\b)", " ", t)  # husstil: intet 'i/af' før materiale
    t = re.sub(r"(?i)\betræ\b", "Egetræ", t)  # garble
    toks = t.split()
    while toks and toks[0].lower().strip(".,-–") in _CONN:
        toks.pop(0)
    while toks and toks[-1].lower().strip(".,-–") in _CONN:
        toks.pop()
    res = []
    for w in toks:
        if res and w.lower().strip(".,-–") in _CONN and res[-1].lower().strip(".,-–") in _CONN:
            continue
        res.append(w)
    t = " ".join(res)
    t = re.sub(r"(?<![\d,.])\s+[A-Za-z]$", "", t)  # trailing enkelt-bogstav ('Bambus T', ikke '6 M')
    return t

# ---------- PUBLIC API ----------
def generate_title(source_title, options, *, shared=True, product_type="", feed_colors=(), n_variants=1):
    """Generér korrekt dansk produkttitel.

    source_title: Shopify-titel ELLER feed-titel kørt gennem titlecase_feed() først.
    options: {optionsnavn: [værdier]} for det FÆRDIGE produkt (efter evt. merge).
    shared: True = delt titel (variant-akser ud). False = single (identitet bevares, kun rens).
    feed_colors: feedets Color-værdier for gruppens SKUs (fanger format-mismatch mod Farve-option).
    Returnerer (titel, needs_llm) — needs_llm 'english'/'sku' → send til LLM-oversættelse (sidste lag).
    """
    opts = {k: list(v) for k, v in (options or {}).items()} if shared else {}
    fcw = set()
    if shared:
        # feed-Color = autoritativ farve-akse — også når Shopify-optionen ikke findes endnu
        # (fx merge af enkelt-produkter i hver sin farve: farven BLIVER en akse efter merge)
        for c in feed_colors:
            c = (c or "").strip()
            if not c:
                continue
            fcw.add(c)
            for w in re.split(r"[\s/]+", c):
                if len(w.strip(".,")) >= 3 and w.lower() != "og":
                    fcw.add(w.strip(".,"))
        if fcw:
            opts["Farve"] = sorted(set(opts.get("Farve", [])) | fcw)
    suggested, _ch, _iss, _rem, needs_llm = analyze(source_title or "", opts, max(n_variants, 1), product_type)
    t = suggested if suggested else (source_title or "")
    if shared:
        if has_dim_axis(options or {}):
            t = strip_dims(t)
        if has_color_axis(options or {}) or fcw:
            t = strip_colors(t, extra_colors=fcw)
        t = strip_numeric_axes(t, options or {})   # antal/panel/kapacitet-akser
    t = strip_malformed_dim(t)
    t = cleanup(t)
    t = dedup_words(t)
    t = final_case(t)
    t = re.sub(r"\s+", " ", t).strip(" ,-–+&/")
    return t, needs_llm
