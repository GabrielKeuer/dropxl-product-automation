"""Opdaterer variant-cost (unitCost) for Sollux-varianter mod NY prisliste (NPP x 7,45).
Kun bogfoering/True Profit - roerer ALDRIG salgspriser. Dry-run default; --apply skriver.
"""
import os, sys, time, argparse
import requests, pandas as pd, warnings
warnings.filterwarnings('ignore')

SHOP = os.environ['SHOPIFY_STORE']; TOK = os.environ['SHOPIFY_ACCESS_TOKEN']
URL = f"https://{SHOP}/admin/api/2024-10/graphql.json"
H = {"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"}
FX = 7.45

def gql(q, v=None):
    for a in range(1, 5):
        r = requests.post(URL, headers=H, json={"query": q, "variables": v or {}}, timeout=120)
        r.raise_for_status(); d = r.json()
        if 'errors' in d:
            if any('hrottl' in str(e) for e in d['errors']) and a < 4: time.sleep(2 ** a); continue
            raise Exception(d['errors'])
        return d

ap = argparse.ArgumentParser(); ap.add_argument('--apply', action='store_true'); args = ap.parse_args()

PRICELIST_URL = 'https://sollux-lighting.com/cenniki2022/SOLLUX_PRICELIST_EUR_EN.xls'
ua = {'User-Agent': 'Mozilla/5.0 (compatible; BoligRetning/1.0)'}
for attempt in range(1, 6):
    try:
        resp = requests.get(PRICELIST_URL, timeout=120, headers=ua); resp.raise_for_status()
        open('SOLLUX_PRICELIST.xls', 'wb').write(resp.content); break
    except Exception as e:
        if attempt == 5: raise
        time.sleep(10 * attempt)
pl = pd.read_excel('SOLLUX_PRICELIST.xls', header=12)
cols = list(pl.columns); sym, npp = cols[6], cols[14]
ny = {}
for _, r in pl.iterrows():
    try: ny[str(r[sym]).strip()] = float(r[npp])
    except (ValueError, TypeError): pass
print(f"prisliste: {len(ny)} SKU'er")

Q = """query($c:String){products(first:60,query:"vendor:Sollux",after:$c){pageInfo{hasNextPage endCursor}
edges{node{id variants(first:40){edges{node{sku inventoryItem{id unitCost{amount}}}}}}}}}"""
M = """mutation($id:ID!,$input:InventoryItemInput!){inventoryItemUpdate(id:$id,input:$input){userErrors{message}}}"""

cur = None; tjekket = 0; diffs = []
while True:
    r = gql(Q, {"c": cur}); pr = r["data"]["products"]
    for e in pr["edges"]:
        for ve in e["node"]["variants"]["edges"]:
            v = ve["node"]; sku = (v["sku"] or "").strip()
            if sku not in ny: continue
            tjekket += 1
            ny_cost = round(ny[sku] * FX, 2)
            gl = float(v["inventoryItem"]["unitCost"]["amount"]) if v["inventoryItem"]["unitCost"] else 0
            if abs(ny_cost - gl) >= 1:
                diffs.append((sku, gl, ny_cost, v["inventoryItem"]["id"]))
    if not pr["pageInfo"]["hasNextPage"]: break
    cur = pr["pageInfo"]["endCursor"]; time.sleep(0.2)

op = [d for d in diffs if d[2] > d[1]]; ned = [d for d in diffs if d[2] < d[1]]
print(f"varianter tjekket: {tjekket} | cost-AFVIGELSER: {len(diffs)} (op: {len(op)} / ned: {len(ned)})")
for d in sorted(diffs, key=lambda x: abs(x[2] - x[1]), reverse=True)[:10]:
    print(f"  {d[0]}: {d[1]:.0f} -> {d[2]:.0f} kr ({'+' if d[2]>d[1] else ''}{d[2]-d[1]:.0f})")

if args.apply:
    ok = fejl = 0
    for sku, gl, nyc, iid in diffs:
        res = gql(M, {"id": iid, "input": {"cost": str(nyc)}})
        err = res["data"]["inventoryItemUpdate"]["userErrors"]
        if err: fejl += 1; print("  FEJL", sku, err)
        else: ok += 1
        time.sleep(0.25)
    print(f"APPLIED: {ok} opdateret, {fejl} fejl")
else:
    print("DRY-RUN (koer med --apply for at skrive)")
