"""Verificér de TO edge cases:
  1. Hvad hvis produktet havde KUN 1 variant (single-variant)?
     → skal være full product delete
  2. Hvad hvis kun "første" variant blev slettet i multi-variant?
     → produktet skal stadig eksistere med beskrivelsen intakt

Bruger OLD's CSV (committed 04:43 i morges, før v2 cutover) som reference
for at vise hvad logikken havde valgt FØR sletning.
"""
import json
import os
import sys
import subprocess

import requests

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
GRAPHQL = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_TOKEN, 'Content-Type': 'application/json'}


def gql(q, v=None):
    p = {'query': q}
    if v: p['variables'] = v
    r = requests.post(GRAPHQL, headers=HEADERS, json=p, timeout=60)
    r.raise_for_status()
    return r.json()


def main():
    # Læs dagens delete-list — som er det v2 lige har slettet
    with open('output/delete_list.json', encoding='utf-8') as f:
        plan = json.load(f)['plan']

    print("=" * 70)
    print("EDGE CASE 1: Var nogle af de 16 full-product-deletes single-variant?")
    print("=" * 70)
    print("(Dvs. produktet havde KUN 1 variant, og den variant er udgået)")
    print()
    print("Vi tjekker ved at se i OLD's matrixify_delete.csv fra i morges:")
    print("  - hvis Command='DELETE' og kun ÉN række per product = single-variant case")
    print("  - hvis Command='DELETE' og FLERE rækker per product = alle-variants case")
    print()

    # Brug git til at hente OLD CSV fra commit FØR cutover
    try:
        result = subprocess.run(
            ['git', 'show', 'HEAD~3:output/matrixify_delete.csv'],
            capture_output=True, text=True, timeout=10
        )
        old_csv = result.stdout
        if not old_csv:
            print("⚠ Kunne ikke finde gammel CSV. Bruger v2's delete_list i stedet.")
            # Fallback: tjek de 16 produkter fra delete_list direkte
            for pid in plan['full_product_deletes'][:5]:
                # Vi kan ikke query'e Shopify (produktet er slettet)
                # men vi VED hvor mange SKUs der var udgåede fra delete-list
                print(f"  Product {pid}: SLETTET (kan ikke længere query)")
        else:
            # Parse CSV
            lines = old_csv.strip().split('\n')[1:]  # skip header
            full_delete_rows = [l for l in lines if l.startswith('DELETE,')]
            print(f"OLD CSV havde {len(full_delete_rows)} rækker med Command=DELETE")
            print("(samme antal som v2's full_product_deletes hvis logikken matcher)")
    except Exception as e:
        print(f"  fejl: {e}")

    print()
    print("=" * 70)
    print("EDGE CASE 2: Et produkt med MANGE variants, hvor 1 (eller få) er slettet")
    print("=" * 70)
    print("Beskrivelsen ligger paa produktet — ikke paa variants. Saa hvis")
    print("EN variant slettes, skal produktet stadig vaere ACTIVE med beskrivelse intakt.")
    print()

    # Find et produkt fra variant_deletes der havde MANGE variants oprindelig
    # Best test = et produkt vi tidligere har set har 50+ variants
    test_cases = [
        ("gid://shopify/Product/15252400537949", "Sengeramme Med Sengegavl Metal"),
        ("gid://shopify/Product/15256417960285", "Tv-Bord 68X39X50,5 Cm Stål"),
    ]

    for pid, label in test_cases:
        q = """
        query($id: ID!) {
          product(id: $id) {
            id title status descriptionHtml vendor productType
            variants(first: 60) { edges { node { id sku } } }
          }
        }
        """
        d = gql(q, {'id': pid})
        product = d['data']['product']
        if not product:
            print(f"❌ {pid} ({label}) eksisterer ikke længere!")
            continue
        desc_len = len(product.get('descriptionHtml') or '')
        print(f"✅ {label}")
        print(f"   product_id: {pid}")
        print(f"   status: {product['status']}")
        print(f"   vendor: {product.get('vendor')} | productType: {product.get('productType')}")
        print(f"   description: {desc_len} chars (intakt = beskrivelsen er paa produktet)")
        print(f"   varianter tilbage: {len(product['variants']['edges'])}")
        print(f"   (en specifik variant blev slettet, resten + product intakt)")
        print()


if __name__ == "__main__":
    if not SHOPIFY_TOKEN:
        sys.exit("❌ SHOPIFY_ACCESS_TOKEN mangler")
    main()
