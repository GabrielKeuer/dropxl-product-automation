"""Print ALLE option-vaerdier for de 4 friske test-produkter — find evt. truncerede/ellipsis vaerdier."""
import os, requests, json

STORE = os.environ['SHOPIFY_STORE']
TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}

HANDLES = [
    "udendoers-sofa-massivt-akacietrae",
    "highboard-695-x-34-x-180-cm-konstrueret-trae-2",
    "pejs-glasplade-glas-2",
    "havesofa-saet-med-opbevaring-6-dele-poly-rattan-4",
]

q = """query($h: String!) {
  productByHandle(handle: $h) {
    title options { name values }
  }
}"""

for h in HANDLES:
    r = requests.post(GRAPHQL, headers=HEADERS,
        json={'query': q, 'variables': {'h': h}}, timeout=30)
    p = r.json().get('data', {}).get('productByHandle')
    if not p:
        print(f"{h}: NOT FOUND")
        continue
    print(f"\n=== {p['title']} ({h}) ===")
    for o in p['options']:
        print(f"  {o['name']} ({len(o['values'])} values):")
        for v in o['values']:
            marker = " ⚠ ELLIPSIS" if '...' in v or '…' in v else ""
            print(f"    - {v!r}{marker}")
