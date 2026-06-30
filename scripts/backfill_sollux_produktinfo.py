"""Backfill: ret variant custom.produktinfo (long+short) + body h4 'ProduktInfo' på
Sollux-produkter oprettet/ændret i denne omgang (i dag) + named extras.
Idempotent: opdaterer kun hvor værdien afviger.
"""
import os, sys, time, argparse
import requests, pandas as pd, warnings
warnings.filterwarnings('ignore'); sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import create_sollux_products as scp
SHOP=os.environ['SHOPIFY_STORE']; TOK=os.environ['SHOPIFY_ACCESS_TOKEN']
URL=f"https://{SHOP}/admin/api/2024-10/graphql.json"; H={"X-Shopify-Access-Token":TOK,"Content-Type":"application/json"}
def gql(q,v=None):
    for a in range(1,5):
        r=requests.post(URL,headers=H,json={"query":q,"variables":v or {}},timeout=120); d=r.json()
        if 'errors' in d:
            if any('hrottl' in str(e) for e in d['errors']) and a<4: time.sleep(2**a); continue
            raise Exception(d['errors'])
        return d

ap=argparse.ArgumentParser(); ap.add_argument('--apply',action='store_true'); ap.add_argument('--today',default='2026-06-30')
args=ap.parse_args()
d,_,_=scp.load_feed()
d['Product name']=d['Product name'].apply(lambda x:scp.apply_fixes(x,scp.TEXT_FIXES))
feedrow={str(r['SKU']).strip():r for _,r in d.iterrows()}

Q="""query($c:String,$q:String!){products(first:60,query:$q,after:$c){pageInfo{hasNextPage endCursor}edges{node{id handle descriptionHtml productType
 variants(first:40){edges{node{id sku metafield(namespace:"custom",key:"produktinfo"){value}}}}}}}}"""
PU="""mutation($id:ID!,$desc:String!){productUpdate(input:{id:$id,descriptionHtml:$desc}){userErrors{message}}}"""
MS="""mutation($mf:[MetafieldsSetInput!]!){metafieldsSet(metafields:$mf){userErrors{message}}}"""

# mål: oprettet i dag (de 96) — query created_at; skala-60 er ændret men oprettet tidligere → tilføj separat
queries=[f"vendor:Sollux created_at:>={args.today}", "vendor:Sollux handle:loftslampe-skala-60"]
seen=set(); body_fix=0; mf_fix=0
for ql in queries:
    cur=None
    while True:
        r=gql(Q,{"c":cur,"q":ql}); pr=r["data"]["products"]
        for e in pr["edges"]:
            n=e["node"]
            if n["id"] in seen: continue
            seen.add(n["id"])
            is_bulb=n["productType"]=="Lyskilde"
            # body h4 case
            desc=n["descriptionHtml"] or ""
            if "Produktinfo</h4>" in desc:
                newdesc=desc.replace("Produktinfo</h4>","ProduktInfo</h4>")
                if args.apply: gql(PU,{"id":n["id"],"desc":newdesc}); 
                body_fix+=1
            # variant produktinfo (skip første variant)
            mfset=[]
            for i,ve in enumerate(n["variants"]["edges"]):
                if i==0: continue
                v=ve["node"]; sku=(v["sku"] or "").strip()
                fr=feedrow.get(sku)
                if fr is None: continue
                correct=scp.produktinfo_html(fr.get('Long description HTML - benefits'), fr.get('Short description HTML'), is_bulb, fr)
                if not correct: continue
                cur_val=(v["metafield"] or {}).get("value")
                if cur_val != correct:
                    mfset.append({"ownerId":v["id"],"namespace":"custom","key":"produktinfo","type":"multi_line_text_field","value":correct})
            if mfset:
                mf_fix+=len(mfset)
                if args.apply:
                    for j in range(0,len(mfset),25):
                        gql(MS,{"mf":mfset[j:j+25]}); time.sleep(0.25)
        if not pr["pageInfo"]["hasNextPage"]: break
        cur=pr["pageInfo"]["endCursor"]; time.sleep(0.2)
print(f"{'APPLIED' if args.apply else 'DRY-RUN'}: body-h4 rettet på {body_fix} produkter | variant-produktinfo rettet på {mf_fix} varianter | produkter scannet {len(seen)}")
