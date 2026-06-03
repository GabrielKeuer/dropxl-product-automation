"""Audit specifikke SKUs vi lige tilfoejede via merge — bekraeft variant images attached."""
import json
import os
import sys
import requests

STORE = os.environ['SHOPIFY_STORE']
TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}

# SKUs vi tilfoejede til de 4 merges i 4. test-run
MERGE_SKUS_TO_CHECK = {
    "modulopbygget-sofaarmlaensendemodul-med-hynder-100-cm": [],  # fyldes fra args
    "pallehynder-2-stk-oxfordstof-bladmonster": [],
    "vaeghaengt-sengebord-2": [],
    "handklaeder-12-stk-360-g-m2-100-bomuld-bordeauxfarvet": [],
}

# Faktiske SKUs fra 4. test-run artifact (manual extract)
MERGE_SKUS_TO_CHECK = {
    "modulopbygget-sofaarmlaensendemodul-med-hynder-100-cm":
        ['4104474', '4104470', '4104477', '4104475', '4104473', '4104468', '4104473', '4104471'],
    "pallehynder-2-stk-oxfordstof-bladmonster": [],  # populate
    "vaeghaengt-sengebord-2": [],
    "handklaeder-12-stk-360-g-m2-100-bomuld-bordeauxfarvet": [],
}


QUERY = """
query($sku: String!) {
  productVariants(first: 5, query: $sku) {
    edges {
      node {
        id sku
        product { handle title }
        image { id url altText }
        media(first: 5) { edges { node { mediaContentType ... on MediaImage { id } } } }
        metafields(first: 5) { edges { node { namespace key value } } }
      }
    }
  }
}
"""


def gql(query, variables=None):
    r = requests.post(GRAPHQL, headers=HEADERS,
                      json={'query': query, 'variables': variables or {}}, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    # Read merge SKUs from latest artifact if passed
    if len(sys.argv) > 1:
        path = sys.argv[1]
        with open(path) as f:
            specs = json.load(f)
        for m in specs.get('merge_specs', [])[:5]:  # match limit=5
            h = m['existing_handle']
            MERGE_SKUS_TO_CHECK[h] = [v['sku'] for v in m['new_variants']]

    total_checked = 0
    total_with_image = 0
    for handle, skus in MERGE_SKUS_TO_CHECK.items():
        if not skus:
            continue
        print(f"\n=== {handle} ({len(skus)} SKUs at tjekke) ===")
        for sku in skus[:5]:  # Tjek max 5 per produkt
            d = gql(QUERY, {'sku': f'sku:{sku}'})
            edges = d.get('data', {}).get('productVariants', {}).get('edges', [])
            if not edges:
                print(f"  ❌ SKU {sku}: NOT FOUND")
                continue
            v = edges[0]['node']
            total_checked += 1
            img = v.get('image')
            has_img = img is not None
            if has_img:
                total_with_image += 1
            mfs = [(e['node']['namespace']+'.'+e['node']['key']) for e in v['metafields']['edges']]
            print(f"  SKU {sku}: image={'✅ ' + img['url'][:60] if has_img else '❌ NONE'}")
            print(f"    metafields: {mfs}")

    print(f"\n=== SUMMARY ===")
    print(f"Variants with image: {total_with_image}/{total_checked}")


if __name__ == "__main__":
    main()
