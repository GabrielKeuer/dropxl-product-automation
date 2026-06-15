# -*- coding: utf-8 -*-
"""FASE 2 — link native variant-image (mediaId) uden upload.

For hver variant uden native image:
  1) match variantens feed-billede (filnavn) mod produktets EKSISTERENDE media -> link.
  2) ellers farve-fallback: link til et media som en søskende-variant i SAMME farve bruger.
  3) ellers: lad være (Shopify/Google falder tilbage på produktbilledet).

Ingen upload, ingen 250-grænse. Idempotent. Bruger productVariantsBulkUpdate(mediaId).
"""
import os, sys, json, time, re, argparse, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import product_utils as pu
try: sys.stdout.reconfigure(encoding="utf-8")
except Exception: pass

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
        try:
            with urllib.request.urlopen(urllib.request.Request(GRAPHQL, data=payload, headers=HEADERS), timeout=120) as r:
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

FETCH = """
query($id: ID!) { product(id: $id) { title
  media(first: 250) { edges { node { ... on MediaImage { id image { url } } } } }
  variants(first: 250) { edges { node { id sku
    selectedOptions { name value }
    image { id }
    media(first: 1) { edges { node { ... on MediaImage { id } } } } } } } } }
"""
BULK_UPDATE = """
mutation($pid: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $pid, variants: $variants) {
    userErrors { field message } } }
"""

def fname(u): return re.sub(r"\?.*", "", u or "").split("/")[-1]

def color_of(opts):
    for o in opts:
        if o["name"].lower() in ("farve", "color", "colour"): return o["value"]
    return None

def build_feed_index():
    feed = pu.fetch_feed(FEED_URL)
    idx = {}
    for _, row in feed.iterrows():
        sku = pu.normalize_sku(row.get("SKU"))
        if not sku: continue
        imgs = pu.get_all_images(row)
        if imgs: idx[sku] = fname(imgs[0])   # kun variantens FØRSTE billede (native)
    print(f"📦 Feed-index: {len(idx)} SKUs")
    return idx

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--ids", nargs="*", default=None)
    ap.add_argument("--products", default=DEFAULT_PRODUCTS)
    args = ap.parse_args()
    if not (STORE and TOKEN and FEED_URL):
        sys.exit(f"❌ Mangler env: STORE={bool(STORE)} TOKEN={bool(TOKEN)} FEED={bool(FEED_URL)}")
    print(f"Repair native variant-image | {'LIVE' if args.live else 'DRY-RUN'} | store {STORE}")

    if args.ids: ids = [str(x) for x in args.ids]
    else: ids = [str(p["id"]) for p in json.load(open(args.products, encoding="utf-8"))]
    if args.limit: ids = ids[:args.limit]
    print(f"🎯 {len(ids)} produkter")
    sku_first = build_feed_index()

    st = {"products": 0, "matched": 0, "colorfb": 0, "left_null": 0, "errors": 0}
    for i, pid in enumerate(ids, 1):
        gid = f"gid://shopify/Product/{pid}"
        try:
            p = gql(FETCH, {"id": gid})["data"]["product"]
            if not p: continue
            # filnavn -> mediaId (eksisterende media på produktet)
            file_media = {}
            for e in p["media"]["edges"]:
                n = e["node"]
                if n and n.get("image"): file_media[fname(n["image"]["url"])] = n["id"]
            vs = [e["node"] for e in p["variants"]["edges"]]
            # farve -> mediaId fra varianter der ALLEREDE har media linket
            color_media = {}
            for v in vs:
                med = v.get("media", {}).get("edges", [])
                if med and med[0].get("node"):
                    c = color_of(v["selectedOptions"])
                    if c and c not in color_media: color_media[c] = med[0]["node"]["id"]
            updates = []
            missing = [v for v in vs if not v.get("image")]
            # 1. eksakt match
            still = []
            for v in missing:
                fn = sku_first.get(pu.normalize_sku(v["sku"]))
                mid = file_media.get(fn) if fn else None
                if mid:
                    updates.append({"id": v["id"], "mediaId": mid}); st["matched"] += 1
                    c = color_of(v["selectedOptions"])
                    if c and c not in color_media: color_media[c] = mid
                else:
                    still.append(v)
            # 2. farve-fallback
            for v in still:
                c = color_of(v["selectedOptions"])
                mid = color_media.get(c) if c else None
                if mid: updates.append({"id": v["id"], "mediaId": mid}); st["colorfb"] += 1
                else: st["left_null"] += 1
            if args.live and updates:
                for j in range(0, len(updates), 25):
                    d = gql(BULK_UPDATE, {"pid": gid, "variants": updates[j:j+25]})
                    errs = d["data"]["productVariantsBulkUpdate"]["userErrors"]
                    if errs: st["errors"] += len(errs); print(f"  [{i}] ⚠ {p['title'][:30]}: {errs[:1]}")
            st["products"] += 1
            if i <= 8 or i % 50 == 0:
                print(f"  [{i}/{len(ids)}] {p['title'][:36]}: link {len(updates)} (match+farve), null {len(missing)-len(updates)}")
        except Exception as e:
            st["errors"] += 1; print(f"  [{i}] ❌ {pid}: {str(e)[:140]}")

    verb = "linket" if args.live else "ville blive linket"
    print(f"\n=== OPSUMMERING ({'LIVE' if args.live else 'DRY-RUN'}) ===")
    print(f"  produkter: {st['products']}")
    print(f"  eksakt match {verb}: {st['matched']}")
    print(f"  farve-fallback {verb}: {st['colorfb']}")
    print(f"  efterladt null (falder tilbage på produktbillede): {st['left_null']}")
    print(f"  fejl: {st['errors']}")

if __name__ == "__main__":
    main()
