"""Slet udgåede Sollux-produkter (alle varianter ude af feedet).

Sikkerhed: default ANALYZE (skriver delete-liste, sletter intet). --apply sletter.
Threshold-gate: afbryd hvis > DELETE_THRESHOLD produkter (beskytter mod feed-fejl).
Variant-niveau udgåelser (enkelt-farver på ellers-levende produkter) håndteres
af create-scriptets merge-sti, IKKE her — her slettes kun HELE udgåede produkter.

Brug:  python delete_sollux_discontinued.py [--apply] [--fetch]
Env:   SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, FEED_DIR (eller --fetch)
"""
import os, sys, time, json, argparse
import requests, pandas as pd, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import create_sollux_products as csp  # genbrug FEED_URLS + fetch_live_feeds

SHOP = os.environ['SHOPIFY_STORE']; TOK = os.environ['SHOPIFY_ACCESS_TOKEN']
URL = f"https://{SHOP}/admin/api/2024-10/graphql.json"
H = {"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"}
DELETE_THRESHOLD = int(os.environ.get('DELETE_THRESHOLD', '200'))

def gql(q, v=None):
    for a in range(1, 5):
        r = requests.post(URL, headers=H, json={"query": q, "variables": v or {}}, timeout=120)
        r.raise_for_status(); d = r.json()
        if 'errors' in d:
            if any('hrottl' in str(e) for e in d['errors']) and a < 4: time.sleep(2**a); continue
            raise Exception(d['errors'])
        return d

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--fetch', action='store_true')
    args = ap.parse_args()
    feed_dir = os.environ.get('FEED_DIR', r'C:\Users\APC\Downloads')
    if args.fetch:
        feed_dir = os.path.join(os.path.dirname(__file__), '..', 'output', 'feed')
        csp.fetch_live_feeds(feed_dir)
    d = pd.read_csv(os.path.join(feed_dir, 'CSV_dunski_EUR_wszystkie.csv'), sep=';', encoding='utf-8', dtype=str)
    feed = {str(s).strip() for s in d['SKU'].dropna()}

    # Hent produkter MED variant-id'er for at kunne slette på variant-niveau
    Q = """query($c:String){products(first:80,query:"vendor:Sollux",after:$c){pageInfo{hasNextPage endCursor}edges{node{id title variants(first:40){edges{node{id sku}}}}}}}"""
    full_del = []   # hele produkter (alle varianter ude af feed)
    var_del = []    # delvise: enkelt-varianter ude af feed (produkt består)
    cur = None
    while True:
        r = gql(Q, {"c": cur}); pr = r["data"]["products"]
        for e in pr["edges"]:
            n = e["node"]
            vs = [(v["node"]["id"], (v["node"]["sku"] or "").strip()) for v in n["variants"]["edges"] if v["node"]["sku"]]
            if not vs: continue
            gone = [vid for vid, sku in vs if sku not in feed]
            if not gone: continue
            if len(gone) == len(vs):
                full_del.append({"id": n["id"], "title": n["title"]})
            else:
                var_del.append({"id": n["id"], "title": n["title"], "variant_ids": gone})
        if not pr["pageInfo"]["hasNextPage"]: break
        cur = pr["pageInfo"]["endCursor"]; time.sleep(0.2)

    out = os.path.join(os.path.dirname(__file__), '..', 'output', 'sollux_delete_list.json')
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump({"full": full_del, "variants": var_del}, open(out, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    var_count = sum(len(p["variant_ids"]) for p in var_del)
    print(f"Udgåede: {len(full_del)} HELE produkter + {var_count} VARIANTER på {len(var_del)} produkter (gemt i {out})")

    if len(full_del) > DELETE_THRESHOLD:
        print(f"⚠ STOP: {len(full_del)} fulde sletninger > threshold {DELETE_THRESHOLD} — mulig feed-fejl.")
        sys.exit(1)
    if not args.apply:
        print("DRY-RUN (analyze). Kør med --apply for at slette."); return

    MP = "mutation($id:ID!){productDelete(input:{id:$id}){deletedProductId userErrors{message}}}"
    MV = "mutation($pid:ID!,$ids:[ID!]!){productVariantsBulkDelete(productId:$pid,variantsIds:$ids){userErrors{message}}}"
    okp = okv = 0
    for p in full_del:
        if gql(MP, {"id": p["id"]})["data"]["productDelete"]["deletedProductId"]: okp += 1
        time.sleep(0.3)
    for p in var_del:
        res = gql(MV, {"pid": p["id"], "ids": p["variant_ids"]})
        if not res["data"]["productVariantsBulkDelete"]["userErrors"]: okv += 1
        time.sleep(0.3)
    print(f"Slettet: {okp} hele produkter + {okv} produkters udgåede varianter")

if __name__ == '__main__':
    main()
