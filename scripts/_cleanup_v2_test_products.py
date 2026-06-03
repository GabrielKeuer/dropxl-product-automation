"""Slet v2 test-produkter + 12 merge-varianter fra Shopify saa vi kan re-teste.

Sletter:
  - 2 nye produkter (productDelete): dyne-med-pude-3-dele-mikrofiber,
    havesofa-saet-9-dele-polyrattan-staal-og-massivt-akacietrae
  - 12 varianter (productVariantsBulkDelete) fra merge-targets

Rydder ogsaa warmup-state i Supabase for de 60 SKUs.
"""
import os
import sys
import requests

STORE = os.environ['SHOPIFY_STORE']
TOKEN = os.environ['SHOPIFY_ACCESS_TOKEN']
GRAPHQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json'}

# Nye produkter at slette helt
NEW_HANDLES = [
    "dyne-med-pude-3-dele-mikrofiber",
    "havesofa-saet-9-dele-polyrattan-staal-og-massivt-akacietrae",
    "kunstigt-juletrae-med-stativ-180-cm-pvc-og-plastik-og-staal",  # 2. test-run partial create
    "udendoers-sofagruppe-med-pude-7-dele-10",                      # 3. test-run
    "have-spisebordssaet-7-dele-massivt-akacietrae",                # 3. test-run
    # 4. test-run
    "zebragardin-120x150-cm-stofbredde-1159-cm-polyester",
    "3-personers-havebaenk-massivt-teaktrae",
    "buede-gabionkurve-6-stk-400x50x100120-cm-galvaniseret-jern",
    "udendoers-sofagruppe-5-dele-naturfarvet-og-lysegraa",
    "havesofasaet-med-16-hynder-polyrattan",
    # 5. test-run
    "sengestel-med-hovedgaerde-beton-konstrueret-trae",
    "frostbeskyttelsesplante-fleece-100-x-16-m-non-woven-stof",
    "rullegardin-uv-beskyttelse-180-x-230-cm-polyester",
    "altanafskaermning-90x300-cm-oxfordstof-moerkegroen",
    "gulvtaeppe-60x100-cm-rektangulaer-bambus-lys-naturfarvet",
]

# Merge-targets + SKUs vi tilfoejede per target
MERGE_DELETES = {
    # 1. test-run merges
    "sengeramme-med-skuffer-konstrueret-trae-brunt-egetrae-1":
        ['3280685', '3280643', '3280699', '3280701', '3280671', '3280640'],
    "sengebord-40x35x47-5-cm-konstrueret-trae":
        ['881978', '881980', '881982', '827267', '827273', '827261'],
    # 2. test-run merges
    "laenestol-med-fodskammel-stof-sort":
        ['3154419', '3154414', '3154418', '3154413', '3154415', '3154417', '3154420'],
    "hojskab-69-5x34x180-cm-konstrueret-trae-21":
        ['3403180', '3403181', '3415634', '3415695', '3415751', '3415789', '3415797',
         '3415861', '3415922', '3415978', '3416016', '3416024', '3416088', '3416147',
         '3416203', '3416241', '3416249', '3198545', '3198229', '3198676', '3198359',
         '3198551', '3198293', '3198484', '3198548', '3198679', '3198230', '3198356',
         '3198353', '3198228', '3198294', '3198358', '3198677', '3198481', '3198225',
         '3198673', '3198357', '3198549', '3198292', '3198231', '3198487', '3198485'],
    # 3. test-run merges (re-tilfoejede de samme 7 til laenestol — er allerede slettet
    # ovenfor i 2. run-cleanup. Den 4. var have-privatliv-skaerm-fretwork som FEJLEDE).

    # 4. test-run merges
    "modulopbygget-sofaarmlaensendemodul-med-hynder-100-cm":
        ['4104397', '4104395', '4104392', '4104423', '4104393', '4104422', '4104457', '4104394'],
    "pallehynder-2-stk-oxfordstof-bladmonster":
        ['360926', '360931', '360933'],
    "vaeghaengt-sengebord-2":
        ['810971', '816952'],
    "handklaeder-12-stk-360-g-m2-100-bomuld-bordeauxfarvet":
        ['137108', '137110', '137097', '137104', '137106', '137107', '137111', '137098',
         '137109', '137102', '137100'],
    # 5. test-run merge (kun sengeramme lykkedes — de 2 andre fejlede)
    "sengeramme-uden-madras-massivt-fyrretrae-61":
        ['3301423', '3301454', '3301456', '3301449', '3301437', '3301421', '3301451',
         '3301445', '3301452', '3301455', '3301438', '3301422', '3301420', '3301450',
         '3301448', '3301425', '3301447', '3301444', '3301446', '3301443', '3301442',
         '3301440'],
}


def gql(query, variables=None):
    r = requests.post(GRAPHQL, headers=HEADERS,
                      json={'query': query, 'variables': variables or {}}, timeout=30)
    r.raise_for_status()
    d = r.json()
    if 'errors' in d:
        print(f"  GraphQL errors: {d['errors']}")
    return d


def find_product(handle):
    q = """query($h: String!) { productByHandle(handle: $h) { id title variants(first: 250) { edges { node { id sku } } } } }"""
    d = gql(q, {'h': handle})
    return d.get('data', {}).get('productByHandle')


def delete_product(product_id):
    q = """mutation($input: ProductDeleteInput!) {
      productDelete(input: $input) { deletedProductId userErrors { field message } }
    }"""
    d = gql(q, {'input': {'id': product_id}})
    return d.get('data', {}).get('productDelete', {})


def delete_variants(product_id, variant_ids):
    q = """mutation($pid: ID!, $vids: [ID!]!) {
      productVariantsBulkDelete(productId: $pid, variantsIds: $vids) {
        product { id variants(first: 1) { edges { node { id } } } }
        userErrors { field message }
      }
    }"""
    d = gql(q, {'pid': product_id, 'vids': variant_ids})
    return d.get('data', {}).get('productVariantsBulkDelete', {})


def main():
    args = sys.argv[1:]
    apply = '--apply' in args

    print(f"=== Cleanup v2 test products ({'APPLY' if apply else 'DRY-RUN'}) ===\n")

    # 1. Slet nye produkter helt
    for h in NEW_HANDLES:
        p = find_product(h)
        if not p:
            print(f"  - {h}: NOT FOUND (allerede slettet?)")
            continue
        v_count = len(p['variants']['edges'])
        print(f"  - {h}: {p['title']!r} ({v_count} variants) — DELETE")
        if apply:
            res = delete_product(p['id'])
            errs = res.get('userErrors') or []
            if errs:
                print(f"    ❌ {errs}")
            else:
                print(f"    ✅ deleted")

    # 2. Slet specifikke varianter fra merge-targets
    for h, skus_to_delete in MERGE_DELETES.items():
        p = find_product(h)
        if not p:
            print(f"\n  - {h}: NOT FOUND")
            continue
        sku_to_vid = {e['node']['sku']: e['node']['id'] for e in p['variants']['edges']}
        target_vids = [sku_to_vid[s] for s in skus_to_delete if s in sku_to_vid]
        missing = [s for s in skus_to_delete if s not in sku_to_vid]
        print(f"\n  - {h}: fundet {len(target_vids)}/{len(skus_to_delete)} variants")
        if missing:
            print(f"    Missing SKUs (allerede slettet?): {missing}")
        if not target_vids:
            continue
        print(f"    Skal slette: {[f'{s}' for s in skus_to_delete if s in sku_to_vid]}")
        if apply:
            res = delete_variants(p['id'], target_vids)
            errs = res.get('userErrors') or []
            if errs:
                print(f"    ❌ {errs}")
            else:
                print(f"    ✅ {len(target_vids)} varianter slettet")

    if not apply:
        print(f"\n[DRY-RUN] Brug --apply for at slette i Shopify")


if __name__ == "__main__":
    main()
