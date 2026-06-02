"""Verificér at delete_products_v2 ramte rigtige targets.

Tjekker:
  - Variant-only deletes: produktet eksisterer STADIG, men den specifikke
    variant er væk; resten af produktets varianter er bevaret.
  - Full product deletes: produktet eksisterer IKKE længere.

Bruger den seneste delete_list.json som sandhedskilde.
"""
import json
import os
import sys

import requests

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}


def gql(query, variables=None):
    payload = {'query': query}
    if variables: payload['variables'] = variables
    r = requests.post(GRAPHQL, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def main():
    with open('output/delete_list.json', encoding='utf-8') as f:
        plan = json.load(f)['plan']

    full_deletes = plan['full_product_deletes'][:3]   # tjek 3
    variant_deletes = list(plan['variant_deletes'].items())[:3]   # tjek 3

    print("=" * 65)
    print("VARIANT-ONLY DELETE VERIFIKATION")
    print("(produktet skal eksistere, kun specifikke variants skal vaere væk)")
    print("=" * 65)
    for product_id, deleted_variant_ids in variant_deletes:
        print(f"\nProduct: {product_id}")
        q = """
        query($id: ID!) {
          product(id: $id) {
            id title status
            variants(first: 50) {
              edges { node { id sku } }
            }
          }
        }
        """
        d = gql(q, {'id': product_id})
        product = d['data']['product']
        if not product:
            print(f"  ❌ Product EKSISTERER IKKE — det er forkert for variant-only delete!")
            continue
        print(f"  ✅ Product eksisterer: {product['title'][:50]} (status={product['status']})")

        remaining_variants = [e['node']['id'] for e in product['variants']['edges']]
        print(f"  Tilbageblevne varianter: {len(remaining_variants)}")

        # Tjek: er de slettede variants VÆK?
        leaks = [v for v in deleted_variant_ids if v in remaining_variants]
        if leaks:
            print(f"  ❌ LEAK: disse variants skulle vaere slettet men er der stadig: {leaks}")
        else:
            print(f"  ✅ Alle slettede variants er VÆK: {deleted_variant_ids}")

    print("\n" + "=" * 65)
    print("FULL PRODUCT DELETE VERIFIKATION")
    print("(produktet maa IKKE eksistere)")
    print("=" * 65)
    for product_id in full_deletes:
        print(f"\nProduct: {product_id}")
        q = "query($id: ID!) { product(id: $id) { id title } }"
        d = gql(q, {'id': product_id})
        if d['data']['product']:
            print(f"  ❌ Product EKSISTERER STADIG: {d['data']['product']['title']}")
        else:
            print(f"  ✅ Product er væk (null returned)")

    print("\n" + "=" * 65)


if __name__ == "__main__":
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")
    main()
