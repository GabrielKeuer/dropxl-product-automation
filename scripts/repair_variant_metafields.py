# -*- coding: utf-8 -*-
"""FASE 1 — reparér manglende custom.variantbilleder-metafelt (variant-galleriet).

For hvert ramt produkt: for hver IKKE-FØRSTE variant der mangler metafeltet,
slå variantens SKU op i vidaXL-feed'et og sæt
  custom.variantbilleder = json.dumps(get_all_images(row))   (hele galleriet).

Feed-drevet (ingen scrape), idempotent (springer varianter der allerede har det),
INGEN upload, INGEN 250-media-grænse (det er bare URLs i et metafelt).
Single-variant + første variant røres ALDRIG (produkt-niveau dækker dem).

Brug:
  python repair_variant_metafields.py                 # DRY-RUN (default)
  python repair_variant_metafields.py --live
  python repair_variant_metafields.py --limit 3 --live # kun N produkter (validering)
  python repair_variant_metafields.py --ids 15464649556317 ...
"""
import os, sys, json, time, argparse, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import product_utils as pu

try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

# --- env (lokalt: læs hub .env.local; CI: os.environ) ---
ENVFILE = r"C:\Users\APC\Desktop\BR\br-ai-hub\br-ai-hub\hub\.env.local"
def _load_local():
    e = {}
    try:
        for line in open(ENVFILE, encoding="utf-8"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1); e[k.strip()] = v.strip().strip('"').strip("'")
    except Exception: pass
    return e
_LE = _load_local()
STORE = (os.environ.get("SHOPIFY_STORE") or _LE.get("SHOPIFY_STORE_URL") or _LE.get("SHOPIFY_STORE") or "").replace("https://", "").replace("http://", "").strip("/")
TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN") or _LE.get("SHOPIFY_ACCESS_TOKEN")
FEED_URL = os.environ.get("FEED_URL") or _LE.get("FEED_URL")
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN or "", "Content-Type": "application/json"}

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PRODUCTS = os.path.join(HERE, "..", "output", "meta3_affected.json")

def gql(query, variables=None):
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    for attempt in range(1, 6):
        req = urllib.request.Request(GRAPHQL, data=payload, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                d = json.loads(r.read().decode())
        except Exception:
            if attempt < 5: time.sleep(2 ** attempt); continue
            raise
        if "errors" in d:
            if any("hrottl" in str(e).lower() for e in d["errors"]) and attempt < 5:
                time.sleep(2 ** attempt); continue
            raise Exception(f"GraphQL errors: {d['errors']}")
        return d
    raise Exception("Max retries")

FETCH_VARIANTS = """
query($id: ID!) { product(id: $id) { title
  variants(first: 250) { edges { node { id sku
    m_sku: metafield(namespace: "custom", key: "sku") { id }
    m_pi: metafield(namespace: "custom", key: "produktinfo") { id }
    m_vb: metafield(namespace: "custom", key: "variantbilleder") { id } } } } } }
"""
METAFIELDS_SET = """
mutation($mf: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $mf) { userErrors { field message } } }
"""

def build_feed_index():
    """sku -> {"imgs": [...], "html": "..."} fra feed (billeder + HTML-beskrivelse)."""
    feed = pu.fetch_feed(FEED_URL)
    idx = {}
    for _, row in feed.iterrows():
        sku = pu.normalize_sku(row.get("SKU"))
        if not sku: continue
        imgs = pu.get_all_images(row)
        html = pu.clean_vidaxl(row.get("HTML_description", ""))
        if imgs or html:
            idx[sku] = {"imgs": imgs, "html": html}
    print(f"📦 Feed-index: {len(idx)} SKUs (billeder/html)")
    return idx

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--ids", nargs="*", default=None, help="Specifikke produkt-ID'er (legacy numerisk)")
    ap.add_argument("--products", default=DEFAULT_PRODUCTS, help="JSON med liste af {id}")
    ap.add_argument("--skip-single", action="store_true",
                    help="Spring single-variant produkter helt over (sæt heller ikke sku-metafelt)")
    args = ap.parse_args()

    if not (STORE and TOKEN and FEED_URL):
        sys.exit(f"❌ Mangler env: STORE={bool(STORE)} TOKEN={bool(TOKEN)} FEED_URL={bool(FEED_URL)}")
    print(f"Repair variantbilleder | {'LIVE' if args.live else 'DRY-RUN'} | store {STORE}")

    if args.ids:
        ids = [str(x) for x in args.ids]
    else:
        data = json.load(open(args.products, encoding="utf-8"))
        ids = [str(p["id"]) for p in data]
    if args.limit: ids = ids[:args.limit]
    print(f"🎯 {len(ids)} produkter i kø")

    sku_imgs = build_feed_index()

    st = {"products": 0, "sku": 0, "pi": 0, "vb": 0, "pi_nofeed": 0, "vb_nofeed": 0,
          "errors": 0, "skipped_single": 0}
    nofeed_skus = []
    for i, pid in enumerate(ids, 1):
        gid = f"gid://shopify/Product/{pid}"
        try:
            d = gql(FETCH_VARIANTS, {"id": gid})
            p = d["data"]["product"]
            if not p:
                print(f"  [{i}] ⚠ produkt {pid} findes ikke"); continue
            vs = [e["node"] for e in p["variants"]["edges"]]   # position-rækkefølge
            if len(vs) <= 1 and args.skip_single:
                st["skipped_single"] += 1; continue
            batch = []
            for idx_v, v in enumerate(vs):
                fd = sku_imgs.get(pu.normalize_sku(v["sku"])) or {}
                # custom.sku — på ALLE varianter (også første/single)
                if not v.get("m_sku") and v.get("sku"):
                    batch.append({"ownerId": v["id"], "namespace": "custom", "key": "sku",
                                  "type": "single_line_text_field", "value": v["sku"]}); st["sku"] += 1
                if idx_v >= 1:   # ikke-første: produktinfo + variantbilleder
                    if not v.get("m_pi"):
                        html = fd.get("html")
                        if html:
                            batch.append({"ownerId": v["id"], "namespace": "custom", "key": "produktinfo",
                                          "type": "multi_line_text_field", "value": html}); st["pi"] += 1
                        else: st["pi_nofeed"] += 1
                    if not v.get("m_vb"):
                        imgs = fd.get("imgs")
                        if imgs:
                            batch.append({"ownerId": v["id"], "namespace": "custom", "key": "variantbilleder",
                                          "type": "list.single_line_text_field", "value": json.dumps(imgs)}); st["vb"] += 1
                        else:
                            st["vb_nofeed"] += 1
                            if len(nofeed_skus) < 30: nofeed_skus.append(v["sku"])
            if args.live and batch:
                for j in range(0, len(batch), 25):
                    dm = gql(METAFIELDS_SET, {"mf": batch[j:j+25]})
                    errs = dm["data"]["metafieldsSet"]["userErrors"]
                    if errs: st["errors"] += len(errs); print(f"  [{i}] ⚠ {p['title'][:30]}: {errs[:1]}")
            st["products"] += 1
            if i <= 8 or i % 50 == 0:
                print(f"  [{i}/{len(ids)}] {p['title'][:38]}: +{len(batch)} metafelt")
        except Exception as e:
            st["errors"] += 1; print(f"  [{i}] ❌ {pid}: {str(e)[:150]}")

    verb = "SAT" if args.live else "ville blive sat"
    print(f"\n=== OPSUMMERING ({'LIVE' if args.live else 'DRY-RUN'}) ===")
    print(f"  produkter behandlet: {st['products']}")
    print(f"  custom.sku {verb}:            {st['sku']}")
    print(f"  custom.produktinfo {verb}:    {st['pi']}  (mangler i feed: {st['pi_nofeed']})")
    print(f"  custom.variantbilleder {verb}: {st['vb']}  (mangler i feed: {st['vb_nofeed']})")
    print(f"  single-variant sprunget over: {st['skipped_single']}")
    print(f"  fejl: {st['errors']}")
    if nofeed_skus:
        print(f"  eksempel SKU uden feed-data: {nofeed_skus[:10]}")

if __name__ == "__main__":
    main()
