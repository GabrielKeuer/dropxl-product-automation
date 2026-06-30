"""Reprice eksisterende Sollux-lamper: tilføj fictive før-pris fra hub-reglen.
Pærer (Lyskilde) røres ikke (ingen før-pris). Pris = uændret (= hub-sale).
Brug: python reprice_sollux_existing.py [--apply]   (default dry-run)
"""
import os, sys, time, argparse
import requests, pandas as pd, warnings
warnings.filterwarnings('ignore'); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pricing
SHOP=os.environ['SHOPIFY_STORE']; TOK=os.environ['SHOPIFY_ACCESS_TOKEN']
URL=f"https://{SHOP}/admin/api/2024-10/graphql.json"; H={"X-Shopify-Access-Token":TOK,"Content-Type":"application/json"}
FX=7.45
def gql(q,v=None):
    for a in range(1,5):
        r=requests.post(URL,headers=H,json={"query":q,"variables":v or {}},timeout=120); r.raise_for_status(); d=r.json()
        if 'errors' in d:
            if any('hrottl' in str(e) for e in d['errors']) and a<4: time.sleep(2**a); continue
            raise Exception(d['errors'])
        return d
ap=argparse.ArgumentParser(); ap.add_argument('--apply',action='store_true'); args=ap.parse_args()
lamp_cfg=pricing.load_pricing_config(vendor='Sollux',product_type=None) or {"mode":"fictive_discount","fixed_markup":1.20/0.55,"rounding":"round_9","fictive_discounts":[20,25,30,35]}
d=pd.read_csv(r"C:\Users\APC\Downloads\CSV_dunski_EUR_wszystkie.csv",sep=';',encoding='utf-8',dtype=str)
gross={str(r['SKU']).strip():float(r['Gross retail price (EUR)']) for _,r in d.iterrows() if pd.notna(r['Gross retail price (EUR)'])}
Q="""query($c:String){products(first:60,query:"vendor:Sollux",after:$c){pageInfo{hasNextPage endCursor}edges{node{id handle productType variants(first:40){edges{node{id sku price compareAtPrice}}}}}}}"""
BU="""mutation($pid:ID!,$v:[ProductVariantsBulkInput!]!){productVariantsBulkUpdate(productId:$pid,variants:$v){userErrors{message}}}"""
cur=None; prod=0; varc=0; skipped_bulb=0; samples=[]
while True:
    r=gql(Q,{"c":cur}); pr=r["data"]["products"]
    for e in pr["edges"]:
        n=e["node"]
        if n["productType"]=="Lyskilde": skipped_bulb+=1; continue   # pærer: ingen før-pris
        updates=[]
        for ve in n["variants"]["edges"]:
            v=ve["node"]; sku=(v["sku"] or "").strip()
            if sku not in gross: continue
            pc=gross[sku]/1.20*0.55*FX
            price,compare=pricing.resolve_variant_pricing(pc,lamp_cfg,seed=n["handle"])
            if compare and compare>price:
                updates.append({"id":v["id"],"price":str(price),"compareAtPrice":str(compare)})
        if updates:
            prod+=1; varc+=len(updates)
            if len(samples)<8: samples.append((n["handle"],[(u["price"],u["compareAtPrice"]) for u in updates[:1]]))
            if args.apply:
                res=gql(BU,{"pid":n["id"],"v":updates})
                err=res["data"]["productVariantsBulkUpdate"]["userErrors"]
                if err: print("  FEJL",n["handle"],err)
                time.sleep(0.3)
    if not pr["pageInfo"]["hasNextPage"]: break
    cur=pr["pageInfo"]["endCursor"]; time.sleep(0.2)
print(f"{'APPLIED' if args.apply else 'DRY-RUN'}: {prod} lampe-produkter, {varc} varianter får før-pris. Pærer sprunget over: {skipped_bulb}")
for h,s in samples: print(f"  {h}: pris/før {s}")
