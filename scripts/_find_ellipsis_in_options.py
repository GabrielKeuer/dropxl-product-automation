"""Søg igennem ALLE vidaXL-produkter for option-vaerdier der indeholder ellipsis ('...' eller '…')."""
import os, requests, json, time

STORE = os.environ['SHOPIFY_STORE']
TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}

q = """query($cursor: String) {
  products(first: 100, after: $cursor, query: "vendor:vidaXL") {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id title handle
        options { name values }
      }
    }
  }
}"""

found = []
total = 0
cursor = None
page = 0
while True:
    page += 1
    r = requests.post(GRAPHQL, headers=HEADERS, json={'query': q, 'variables': {'cursor': cursor}}, timeout=30)
    d = r.json()
    if 'errors' in d:
        print(f"errors: {d['errors']}")
        break
    products_data = d.get('data', {}).get('products', {})
    edges = products_data.get('edges', [])
    for e in edges:
        p = e['node']
        total += 1
        for o in p['options']:
            for v in o['values']:
                if '...' in v or '…' in v:
                    found.append({'handle': p['handle'], 'option': o['name'], 'value': v})
    page_info = products_data.get('pageInfo', {})
    if not page_info.get('hasNextPage'):
        break
    cursor = page_info.get('endCursor')
    if page % 10 == 0:
        print(f"  scanned {total} products so far, {len(found)} hits")
    time.sleep(0.2)

print(f"\nTotal scanned: {total} produkter")
print(f"Found {len(found)} option values with ellipsis:")
for f in found[:30]:
    print(f"  {f['handle']}: option={f['option']!r}, value={f['value']!r}")
if len(found) > 30:
    print(f"  ... og {len(found)-30} flere")
