"""LLM-REPAIR-LAG for titler (sidste lag oven på den deterministiske title_engine).

Bruges af create/merge: efter generate_title() reviewer en LLM titlen MED de reelle
variant-akser som kontekst (så den — modsat en blind dommer — ved hvad der er akse vs.
fast identitet) og retter kun hvis nødvendigt. Regex-gate afviser usikre/aksegenindførende
rettelser. GRACEFUL: manglende API-nøgle eller fejl → deterministisk titel beholdes (blokerer
ALDRIG oprettelse). Slå fra med TITLE_LLM_REPAIR=0.
"""
from __future__ import annotations
import json, os, re, time

MODEL = "claude-sonnet-5"
BATCH = 15
_CONN_END = {"og", "med", "til", "i", "på", "af", "samt", "for", "uden", "x", "+", "&"}

SYS = (
    "Du er dansk produkttitel-korrektør for en møbel-webshop. For hvert produkt får du: feed-titel (rå kilde), "
    "de REELLE variant-akser (navn + værdier) og en genereret DELT titel.\n"
    "Den delte titel deles af alle varianter. REGLER:\n"
    "- Variant-akse-VÆRDIER må IKKE stå i titlen (kunden vælger dem). Fjern rester.\n"
    "- Attributter der IKKE er en akse (fast farve/størrelse/antal) SKAL blive stående — det er identitet.\n"
    "- Opfind ALDRIG nyt. Bevar hovedord (produkttype). Ret klodset dansk/rester/garble.\n"
    "- Husets stil: intet 'i'/'af' før materiale ('Sofabord Massivt Akacietræ'); 'Konstrueret Træ' er korrekt; "
    "'N Dele' i sæt-navne er identitet MED MINDRE 'Antal'/'Dele' er en akse; Title Case; behold LED/PVC/MDF.\n"
    "- Er titlen allerede korrekt: fix=null.\n"
    'Svar KUN JSON: [{"i":1,"fix":"Rettet Titel"|null}, ...]'
)


def _post(records, api_key):
    import urllib.request
    lines = []
    for it in records:
        axes = "; ".join(f"{k}={','.join(str(x) for x in v[:6])}" for k, v in (it.get("axes") or {}).items()) or "ingen"
        lines.append(f'{it["i"]}. feed="{it.get("feed","")[:90]}" | akser: {axes} | titel="{it["det"]}"')
    body = {"model": MODEL, "max_tokens": 3000, "system": SYS,
            "messages": [{"role": "user", "content": "\n".join(lines)}]}
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode(),
                                 headers={"x-api-key": api_key, "anthropic-version": "2023-06-01",
                                          "content-type": "application/json"})
    for a in range(1, 5):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                d = json.loads(r.read().decode())
            txt = "".join(b.get("text", "") for b in d.get("content", []))
            return {int(o["i"]): o.get("fix") for o in json.loads(re.search(r"\[.*\]", txt, re.S).group(0))}
        except Exception:
            if a >= 4:
                return {}
            time.sleep(2 * a)
    return {}


def _reintroduces_axis(fix, axes, det):
    for vals in (axes or {}).values():
        for v in vals:
            v = str(v).strip()
            if len(v) >= 2 and re.search(r"\b" + re.escape(v) + r"\b", fix, re.I) \
                    and not re.search(r"\b" + re.escape(v) + r"\b", det, re.I):
                return True
    return False


def _gate(fix, det, axes):
    if not fix or not isinstance(fix, str):
        return False
    f = fix.strip()
    if len(f) < 3 or not re.search(r"[A-Za-zÆØÅæøå]{3}", f):
        return False
    if re.search(r"(?i)vidaxl|Ã|â€", f) or "  " in f:
        return False
    if f.split()[-1].lower().strip(".,-–") in _CONN_END:
        return False
    if len(f) > len(det) + 25:            # anti-hallucination: rettelse må ikke blive meget længere
        return False
    if _reintroduces_axis(f, axes, det):  # må ikke genindføre en akse-værdi
        return False
    return True


def repair_titles(records, api_key=None):
    """records: [{i, feed, det, axes:{name:[values]}, ptype}]. Returnerer {i: sikker_rettet_titel}.
    Kun ændrede+gated titler er i output; resten falder tilbage til deterministisk (det)."""
    if os.environ.get("TITLE_LLM_REPAIR", "1") == "0":
        return {}
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key or not records:
        return {}
    by_i = {r["i"]: r for r in records}
    out = {}
    for b0 in range(0, len(records), BATCH):
        chunk = records[b0:b0 + BATCH]
        res = _post(chunk, api_key)
        for i, fix in res.items():
            r = by_i.get(i)
            if r and fix and fix != r["det"] and _gate(fix, r["det"], r.get("axes")):
                out[i] = fix
    return out


def repair_one(feed, det, axes, api_key=None):
    """Bekvemmeligheds-wrapper for en enkelt titel (fx merge-regen). Returnerer rettet eller det."""
    r = repair_titles([{"i": 1, "feed": feed, "det": det, "axes": axes}], api_key)
    return r.get(1, det)
