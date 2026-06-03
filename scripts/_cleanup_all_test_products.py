"""MASTER cleanup: download alle 9 test-artifacts og slet alle test-produkter.

Henter product_specs_v2.json fra hvert test-run, samler:
  - Alle 'product_specs' handles (helt-nye produkter) -> productDelete
  - Alle 'merge_specs' new_variants SKUs (merge-tilfoejet) -> productVariantsBulkDelete

Brug --apply for faktisk sletning.
"""
import json
import os
import sys
import requests
from pathlib import Path

STORE = os.environ['SHOPIFY_STORE']
TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}


def gql(q, v=None):
    r = requests.post(GRAPHQL, headers=HEADERS,
                      json={'query': q, 'variables': v or {}}, timeout=30)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        print(f"  errors: {d['errors']}")
    return d


def find_product(handle):
    q = """query($h: String!) { productByHandle(handle: $h) { id title variants(first: 250) { edges { node { id sku } } } } }"""
    return gql(q, {'h': handle}).get('data', {}).get('productByHandle')


def delete_product(pid):
    q = """mutation($input: ProductDeleteInput!) {
      productDelete(input: $input) { deletedProductId userErrors { field message } }
    }"""
    return gql(q, {'input': {'id': pid}}).get('data', {}).get('productDelete', {})


def delete_variants(pid, vids):
    q = """mutation($pid: ID!, $vids: [ID!]!) {
      productVariantsBulkDelete(productId: $pid, variantsIds: $vids) {
        userErrors { field message }
      }
    }"""
    return gql(q, {'pid': pid, 'vids': vids}).get('data', {}).get('productVariantsBulkDelete', {})


def main():
    apply = '--apply' in sys.argv

    artifact_dir = Path('all_artifacts')
    if not artifact_dir.exists():
        sys.exit("artifacts dir mangler — workflow skal downloade dem foerst")

    # Saml fra alle artifact JSONs
    all_new_handles = set()
    all_merge_skus = {}  # handle -> list of SKUs

    for path in artifact_dir.glob('**/product_specs_v2.json'):
        print(f"  Indlæser: {path}")
        with open(path, 'r', encoding='utf-8') as f:
            specs = json.load(f)
        # Bemærk: limit i test-runs gør at kun N foerste blev faktisk pushet,
        # men vi prøver at slette ALLE for sikkerhed (NOT FOUND er OK).
        for p in specs.get('product_specs', []):
            all_new_handles.add(p['handle'])
        for m in specs.get('merge_specs', []):
            h = m['existing_handle']
            skus = [v['sku'] for v in m['new_variants']]
            all_merge_skus.setdefault(h, []).extend(skus)

    print(f"\n📊 Samlet: {len(all_new_handles)} nye produkter, {len(all_merge_skus)} merge-targets")

    # Slet nye produkter
    deleted_products = 0
    for h in sorted(all_new_handles):
        p = find_product(h)
        if not p:
            print(f"  - {h}: NOT FOUND")
            continue
        n = len(p['variants']['edges'])
        if apply:
            r = delete_product(p['id'])
            errs = r.get('userErrors') or []
            if errs:
                print(f"  - {h}: ❌ {errs}")
            else:
                deleted_products += 1
                print(f"  - {h}: ✅ deleted ({n} variants)")
        else:
            print(f"  - {h}: ({n} variants) — WOULD DELETE")

    # Slet merge variants
    deleted_variants = 0
    for h, skus in all_merge_skus.items():
        p = find_product(h)
        if not p:
            print(f"  MERGE - {h}: NOT FOUND")
            continue
        sku_to_vid = {e['node']['sku']: e['node']['id'] for e in p['variants']['edges']}
        target_vids = [sku_to_vid[s] for s in set(skus) if s in sku_to_vid]
        if not target_vids:
            print(f"  MERGE - {h}: 0 variants to delete (already cleaned)")
            continue
        if apply:
            r = delete_variants(p['id'], target_vids)
            errs = r.get('userErrors') or []
            if errs:
                print(f"  MERGE - {h}: ❌ {errs}")
            else:
                deleted_variants += len(target_vids)
                print(f"  MERGE - {h}: ✅ {len(target_vids)} variants deleted")
        else:
            print(f"  MERGE - {h}: {len(target_vids)} variants — WOULD DELETE")

    print(f"\n📊 SUMMARY: {deleted_products} produkter slettet, {deleted_variants} merge-variants slettet")


if __name__ == "__main__":
    main()
