"""Audit: query Shopify for the 4 test products + 1 reference product created by v1.
Compare what v1 INTENDS to write vs what v2 actually pushed.

Usage (GitHub Action): python scripts/_audit_v1_vs_v2.py
"""
import json
import os
import sys
import requests

STORE = os.environ['SHOPIFY_STORE']
TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}

# Current v2 test products (4th run — publish + options_to_add fix)
HANDLES = [
    "havesofasaet-med-16-hynder-polyrattan",                              # v2 NEW (2 variants, 1 option)
    "zebragardin-120x150-cm-stofbredde-1159-cm-polyester",                # v2 NEW (1 variant)
    "handklaeder-12-stk-360-g-m2-100-bomuld-bordeauxfarvet",              # v2 MERGE (+11 variants)
    "modulopbygget-sofaarmlaensendemodul-med-hynder-100-cm",              # v2 MERGE (+8 variants)
]

# Pick a recent v1-created product to see how Matrixify-generated products look
# (We'll query Shopify for any vidaXL product created in last 30 days)

QUERY_PRODUCT = """
query($handle: String!) {
  productByHandle(handle: $handle) {
    id title handle status vendor productType tags descriptionHtml createdAt
    seo { title description }
    onlineStoreUrl publishedAt
    options { id name values }
    media(first: 50) {
      edges { node { mediaContentType ... on MediaImage { id alt image { url } } } }
    }
    metafields(first: 30) {
      edges { node { namespace key type value } }
    }
    resourcePublicationsV2(first: 10) {
      edges { node { publication { name } isPublished } }
    }
    variants(first: 100) {
      edges {
        node {
          id sku price compareAtPrice barcode position
          selectedOptions { name value }
          inventoryPolicy taxable
          image { id url altText }
          media(first: 5) { edges { node { mediaContentType ... on MediaImage { id image { url } } } } }
          metafields(first: 20) { edges { node { namespace key type value } } }
          inventoryItem {
            id tracked requiresShipping
            unitCost { amount currencyCode }
            measurement { weight { value unit } }
          }
        }
      }
    }
  }
}
"""

QUERY_REF = """
query {
  products(first: 5, query: "vendor:vidaXL created_at:>=2026-05-25 status:active") {
    edges { node { id handle title } }
  }
}
"""


def gql(query, variables=None):
    payload = {'query': query}
    if variables: payload['variables'] = variables
    r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        print(f"GraphQL ERRORS: {json.dumps(d['errors'], indent=2)}")
    return d


def audit_product(handle):
    d = gql(QUERY_PRODUCT, {'handle': handle})
    p = d.get('data', {}).get('productByHandle')
    if not p:
        print(f"  ❌ Product {handle!r} not found")
        return None

    print(f"\n{'='*70}")
    print(f"PRODUCT: {p['title']!r}")
    print(f"Handle: {p['handle']}")
    print(f"Status: {p['status']}  publishedAt: {p['publishedAt']}")
    print(f"Vendor: {p['vendor']}  Type: {p['productType']}")
    print(f"SEO title: {p['seo']['title']!r}")
    print(f"SEO desc: {(p['seo']['description'] or '')[:80]!r}")
    print(f"Description: {len(p['descriptionHtml'] or '')} chars")
    print(f"Tags: {len(p['tags'])} — first 6: {p['tags'][:6]}")

    print(f"\nOptions: {[(o['name'], len(o['values'])) for o in p['options']]}")

    media = [e['node'] for e in p['media']['edges']]
    print(f"\nProduct-level media: {len(media)}")
    for m in media[:3]:
        alt = m.get('alt', '') or '(no alt)'
        url = (m.get('image', {}) or {}).get('url', '')[:80]
        print(f"  - alt={alt[:50]!r} url={url}")

    mfs = [e['node'] for e in p['metafields']['edges']]
    print(f"\nProduct-level metafields: {len(mfs)}")
    for m in mfs:
        print(f"  - {m['namespace']}.{m['key']} ({m['type']}) value={(m['value'] or '')[:60]!r}")

    pubs = [e['node'] for e in p.get('resourcePublicationsV2', {}).get('edges', [])]
    print(f"\nPublications: {len(pubs)}")
    for pub in pubs:
        print(f"  - {pub['publication']['name']}: published={pub['isPublished']}")

    variants = [e['node'] for e in p['variants']['edges']]
    print(f"\nVariants: {len(variants)}")
    for v in variants[:3]:
        sel = ', '.join(f"{s['name']}={s['value']}" for s in v['selectedOptions'])
        print(f"\n  --- Variant {v['sku']} [{sel}] ---")
        print(f"    Position: {v.get('position')}")
        print(f"    Price={v['price']} compareAt={v['compareAtPrice']} barcode={v['barcode']}")
        print(f"    InvPolicy: {v.get('inventoryPolicy')}  Taxable: {v.get('taxable')}")
        img = v.get('image')
        if img:
            print(f"    Variant image: id={img.get('id')} altText={img.get('altText')!r} url={img['url'][:80]}")
        else:
            print(f"    Variant image: NONE ❌")

        v_media = [e['node'] for e in v.get('media', {}).get('edges', [])]
        print(f"    Variant media: {len(v_media)}")

        v_mfs = [e['node'] for e in v['metafields']['edges']]
        print(f"    Variant metafields: {len(v_mfs)}")
        for m in v_mfs:
            val = (m['value'] or '')[:80]
            print(f"      - {m['namespace']}.{m['key']} ({m['type']}) = {val!r}")

        ii = v.get('inventoryItem', {})
        cost = (ii.get('unitCost') or {}).get('amount')
        wm = (ii.get('measurement') or {}).get('weight') or {}
        print(f"    InventoryItem: tracked={ii.get('tracked')} reqShip={ii.get('requiresShipping')} "
              f"cost={cost} weight={wm.get('value')}{wm.get('unit')}")
    if len(variants) > 3:
        print(f"\n  ... og {len(variants)-3} flere varianter")

    return p


def main():
    print("=" * 70)
    print("AUDIT: v2 test products")
    print("=" * 70)
    for h in HANDLES:
        audit_product(h)

    # Find a v1 reference product to compare against
    print("\n" + "=" * 70)
    print("REFERENCE: Recent v1-created vidaXL product")
    print("=" * 70)
    d = gql(QUERY_REF)
    edges = d.get('data', {}).get('products', {}).get('edges', [])
    if edges:
        # Pick the first one
        ref_handle = edges[0]['node']['handle']
        print(f"Found v1 product: {ref_handle}")
        audit_product(ref_handle)
    else:
        print("No recent v1 vidaXL products found")


if __name__ == "__main__":
    main()
