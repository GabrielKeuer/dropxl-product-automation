# -*- coding: utf-8 -*-
"""FASE 2b — rebalancér media så de SIDSTE null-varianter får native image.

For hvert produkt med null-varianter:
  1) find hver null-variants ønskede billede (feed[0]).
  2) hvis det allerede er på produktet -> link (sjældent, exact-match burde have fanget).
  3) ellers skal det UPLOADES. Hvis ikke plads (250) -> slet et antal ULINKEDE
     (redundante) media for at frigøre pladser. Linkede media (en variants native
     image) slettes ALDRIG. Galleriet (variantbilleder-metafelt = feed-URLs) påvirkes ikke.
  4) upload de manglende billeder + link varianterne.

Dry-run (default) viser præcis trim/upload/link pr. produkt. --live udfører.
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
DEFAULT_PRODUCTS = os.path.join(HERE, "..", "output", "null_products.json")

def gql(query, variables=None):
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    for attempt in range(1, 6):
        try:
            with urllib.request.urlopen(urllib.request.Request(GRAPHQL, data=payload, headers=HEADERS), timeout=180) as r:
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
  variants(first: 250) { edges { node { id sku image { id }
    media(first: 1) { edges { node { ... on MediaImage { id } } } } } } } } }
"""
DELETE_MEDIA = 'mutation($pid: ID!, $ids: [ID!]!) { productDeleteMedia(productId: $pid, mediaIds: $ids) { deletedMediaIds mediaUserErrors { message } } }'
CREATE_MEDIA = 'mutation($pid: ID!, $media: [CreateMediaInput!]!) { productCreateMedia(productId: $pid, media: $media) { media { id alt } mediaUserErrors { message } } }'
BULK_UPDATE = 'mutation($pid: ID!, $variants: [ProductVariantsBulkInput!]!) { productVariantsBulkUpdate(productId: $pid, variants: $variants) { userErrors { message } } }'

def fname(u): return re.sub(r"\?.*", "", u or "").split("/")[-1]

def build_feed_index():
    feed = pu.fetch_feed(FEED_URL)
    idx = {}
    for _, row in feed.iterrows():
        sku = pu.normalize_sku(row.get("SKU"))
        if not sku: continue
        imgs = pu.get_all_images(row)
        if imgs: idx[sku] = imgs[0]   # variantens FØRSTE billede (native)
    print(f"📦 Feed-index: {len(idx)} SKUs")
    return idx

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--ids", nargs="*", default=None)
    ap.add_argument("--products", default=DEFAULT_PRODUCTS)
    args = ap.parse_args()
    if not (STORE and TOKEN and FEED_URL): sys.exit("❌ Mangler env")
    print(f"Rebalance media | {'LIVE' if args.live else 'DRY-RUN'} | store {STORE}")
    if args.ids: ids = [str(x) for x in args.ids]
    else: ids = [str(p["id"]) for p in json.load(open(args.products, encoding="utf-8"))]
    sku_first = build_feed_index()

    st = {"products": 0, "trimmed": 0, "uploaded": 0, "linked": 0, "skipped_test": 0, "errors": 0}
    for i, pid in enumerate(ids, 1):
        gid = f"gid://shopify/Product/{pid}"
        try:
            p = gql(FETCH, {"id": gid})["data"]["product"]
            if not p: continue
            if "test produkt" in p["title"].lower(): st["skipped_test"] += 1; continue
            media = [(e["node"]["id"], fname((e["node"].get("image") or {}).get("url", "")))
                     for e in p["media"]["edges"] if e["node"]]
            file_media = {fn: mid for mid, fn in media}
            vs = [e["node"] for e in p["variants"]["edges"]]
            in_use = set()
            for v in vs:
                med = v.get("media", {}).get("edges", [])
                if med and med[0].get("node"): in_use.add(med[0]["node"]["id"])
            nulls = [v for v in vs if not v.get("image")]
            # ønskede uploads (dedup på filnavn): null-varianter hvis billede IKKE er på produktet
            need = {}   # filnavn -> (url, [variant_ids])
            link_existing = []  # (variant_id, mediaId) hvis billedet faktisk er der
            for v in nulls:
                url = sku_first.get(pu.normalize_sku(v["sku"]))
                if not url: continue
                fn = fname(url)
                if fn in file_media:
                    link_existing.append((v["id"], file_media[fn]))
                else:
                    need.setdefault(fn, [url, []])[1].append(v["id"])
            headroom = 250 - len(media)
            trim_n = max(0, len(need) - headroom)
            # ulinkede media (kandidater til trim) — ALDRIG linkede
            unlinked = [mid for mid, fn in media if mid not in in_use]
            trim_n = min(trim_n, len(unlinked))
            print(f"  [{i}] {p['title'][:34]}: null={len(nulls)} | upload={len(need)} | trim={trim_n} (ulinkede={len(unlinked)}) | link-eksist={len(link_existing)}")
            if not args.live:
                st["products"] += 1; st["trimmed"] += trim_n; st["uploaded"] += len(need); st["linked"] += len(need) + len(link_existing)
                continue
            # LIVE: 1) trim
            if trim_n:
                ids_del = unlinked[:trim_n]
                d = gql(DELETE_MEDIA, {"pid": gid, "ids": ids_del})
                st["trimmed"] += len(d["data"]["productDeleteMedia"]["deletedMediaIds"])
            # 2) upload
            new_media = {}
            if need:
                media_in = [{"originalSource": u, "mediaContentType": "IMAGE", "alt": f"{p['title']} variant"} for fn,(u,_) in need.items()]
                # productCreateMedia matcher response-rækkefølge til input
                dm = gql(CREATE_MEDIA, {"pid": gid, "media": media_in})
                created = dm["data"]["productCreateMedia"]["media"] or []
                for (fn,(u,vids)), m in zip(need.items(), created):
                    new_media[fn] = m["id"]; st["uploaded"] += 1
            # 3) link (vent kort så media er processeret)
            time.sleep(2)
            updates = list(link_existing)
            for fn,(u,vids) in need.items():
                mid = new_media.get(fn)
                if mid:
                    for vid in vids: updates.append((vid, mid))
            for j in range(0, len(updates), 25):
                chunk = [{"id": vid, "mediaId": mid} for vid, mid in updates[j:j+25]]
                du = gql(BULK_UPDATE, {"pid": gid, "variants": chunk})
                errs = du["data"]["productVariantsBulkUpdate"]["userErrors"]
                if errs: st["errors"] += len(errs); print(f"      ⚠ link: {errs[:1]}")
                else: st["linked"] += len(chunk)
            st["products"] += 1
        except Exception as e:
            st["errors"] += 1; print(f"  [{i}] ❌ {pid}: {str(e)[:140]}")

    print(f"\n=== OPSUMMERING ({'LIVE' if args.live else 'DRY-RUN'}) ===")
    print(f"  produkter: {st['products']} | test sprunget over: {st['skipped_test']}")
    print(f"  media trimmet: {st['trimmed']} | uploadet: {st['uploaded']} | varianter linket: {st['linked']}")
    print(f"  fejl: {st['errors']}")

if __name__ == "__main__":
    main()
