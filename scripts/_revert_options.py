"""Revert 2 half-mutated produkter: slet Farve+Model/Stoerrelse options
som productOptionsCreate tilfoejede foer variant-create fejlede.
"""
import os
import requests
import sys

STORE = os.environ['SHOPIFY_STORE']
TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}

# Half-mutated produkter — options vi tilfoejede der skal fjernes
REVERT_TARGETS = {
    "10-delt-havemobelsaet-med-puder-sort-poly-rattan": ["Farve", "Model"],
    "sengestel-med-hovedgaerde-sonoma-160-x-200-cm-massiv-fyrretrae": ["Farve", "Størrelse"],
}


def gql(query, variables=None):
    r = requests.post(GRAPHQL, headers=HEADERS,
                      json={'query': query, 'variables': variables or {}}, timeout=30)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        print(f"  errors: {d['errors']}")
    return d


def main():
    apply = '--apply' in sys.argv
    print(f"=== Revert ({'APPLY' if apply else 'DRY-RUN'}) ===\n")

    for handle, options_to_remove in REVERT_TARGETS.items():
        # Find product + dets option-IDs
        q = """query($h: String!) {
          productByHandle(handle: $h) {
            id title options { id name values }
          }
        }"""
        d = gql(q, {'h': handle})
        p = d.get('data', {}).get('productByHandle')
        if not p:
            print(f"  ❌ {handle}: not found")
            continue

        cur_opts = p['options']
        to_delete_ids = [o['id'] for o in cur_opts if o['name'] in options_to_remove]
        print(f"  {handle}:")
        print(f"    Current options: {[(o['name'], o['values']) for o in cur_opts]}")
        print(f"    Vil fjerne {len(to_delete_ids)} options: {options_to_remove}")
        if not to_delete_ids:
            print(f"    -> ingen at fjerne (allerede revertet?)")
            continue

        if apply:
            m = """mutation($pid: ID!, $opts: [ID!]!) {
              productOptionsDelete(productId: $pid, options: $opts, strategy: LEAVE_AS_IS) {
                deletedOptionsIds userErrors { field message code }
              }
            }"""
            d2 = gql(m, {'pid': p['id'], 'opts': to_delete_ids})
            res = d2.get('data', {}).get('productOptionsDelete', {})
            errs = res.get('userErrors') or []
            if errs:
                print(f"    ❌ {errs}")
            else:
                print(f"    ✅ deleted {len(res.get('deletedOptionsIds') or [])} options")


if __name__ == "__main__":
    main()
