import requests
import time
import os
import sys

print("VidaXL Channel Publisher - Publicer til alle salgskanaler")
print("=" * 60)

SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')

if not SHOPIFY_STORE or not SHOPIFY_ACCESS_TOKEN:
    print("❌ Manglende SHOPIFY_STORE eller SHOPIFY_ACCESS_TOKEN")
    sys.exit(1)

URL = f"https://{SHOPIFY_STORE}/admin/api/2024-10/graphql.json"
HEADERS = {'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN, 'Content-Type': 'application/json'}


def graphql(query, variables=None):
    """Kør GraphQL query med retry ved throttling"""
    payload = {'query': query}
    if variables:
        payload['variables'] = variables

    for attempt in range(3):
        resp = requests.post(URL, headers=HEADERS, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if 'errors' in data:
            if any('Throttled' in str(e) for e in data['errors']):
                time.sleep(2)
                continue
            raise Exception(f"GraphQL fejl: {data['errors']}")

        ext = data.get('extensions', {}).get('cost', {}).get('throttleStatus', {})
        if ext.get('currentlyAvailable', 1000) < 100:
            time.sleep(1)

        return data
    raise Exception("Max retries exceeded")


def get_all_publications():
    """Hent alle tilgængelige salgskanaler/publications"""
    print("\n📡 Henter salgskanaler...")

    data = graphql('''
    {
        publications(first: 50) {
            edges {
                node {
                    id
                    name
                }
            }
        }
    }
    ''')

    pubs = []
    for edge in data.get('data', {}).get('publications', {}).get('edges', []):
        node = edge['node']
        pubs.append({'id': node['id'], 'name': node['name']})
        print(f"   ✅ {node['name']}")

    print(f"\n✅ {len(pubs)} salgskanaler fundet")
    return pubs


def get_unpublished_products(publications):
    """Find vidaXL produkter der mangler en eller flere kanaler"""
    print("\n🔍 Finder vidaXL produkter der mangler kanaler...")

    products_to_publish = []
    has_next = True
    cursor = None
    total_checked = 0

    while has_next:
        after = f', after: "{cursor}"' if cursor else ''
        data = graphql(f'''
        {{
            products(first: 50, query: "vendor:vidaXL"{after}) {{
                edges {{
                    node {{
                        id
                        title
                        publishedOnPublicationCount
                    }}
                    cursor
                }}
                pageInfo {{ hasNextPage }}
            }}
        }}
        ''')

        edges = data.get('data', {}).get('products', {}).get('edges', [])
        for edge in edges:
            node = edge['node']
            pub_count = node.get('publishedOnPublicationCount', 0)

            # Hvis produktet ikke er på alle kanaler
            if pub_count < len(publications):
                products_to_publish.append({
                    'id': node['id'],
                    'title': node['title'],
                    'current_channels': pub_count
                })

        total_checked += len(edges)
        pi = data.get('data', {}).get('products', {}).get('pageInfo', {})
        has_next = pi.get('hasNextPage', False)
        if has_next and edges:
            cursor = edges[-1].get('cursor')

        if total_checked % 500 == 0:
            print(f"   Tjekket {total_checked} produkter, {len(products_to_publish)} mangler kanaler...")

    print(f"✅ Tjekket {total_checked} vidaXL produkter")
    print(f"   {len(products_to_publish)} mangler en eller flere kanaler")
    return products_to_publish


def publish_to_all_channels(products, publications):
    """Publicer produkter til alle kanaler"""
    if not products:
        print("\n✅ Alle produkter er allerede på alle kanaler!")
        return 0

    print(f"\n📢 Publicerer {len(products)} produkter til alle {len(publications)} kanaler...")

    pub_inputs = [{'publicationId': p['id']} for p in publications]
    published = 0
    failed = 0

    for i, product in enumerate(products):
        try:
            data = graphql('''
            mutation publishProduct($id: ID!, $input: [PublicationInput!]!) {
                publishablePublish(id: $id, input: $input) {
                    publishable {
                        ... on Product {
                            id
                        }
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
            ''', variables={
                'id': product['id'],
                'input': pub_inputs
            })

            errors = data.get('data', {}).get('publishablePublish', {}).get('userErrors', [])
            if errors:
                # Filtrer "already published" fejl — de er OK
                real_errors = [e for e in errors if 'already' not in e.get('message', '').lower()]
                if real_errors:
                    print(f"   ⚠️ {product['title'][:50]}: {real_errors[0]['message']}")
                    failed += 1
                else:
                    published += 1
            else:
                published += 1

            if (i + 1) % 50 == 0:
                print(f"   Publiceret {i + 1}/{len(products)}...")

            time.sleep(0.3)

        except Exception as e:
            print(f"   ❌ Fejl: {product['title'][:50]}: {str(e)[:80]}")
            failed += 1
            time.sleep(1)

    print(f"\n✅ Færdig! Publiceret: {published}, Fejlet: {failed}")
    return published


# ============================================================
# HOVEDPROGRAM
# ============================================================

try:
    publications = get_all_publications()

    if not publications:
        print("❌ Ingen salgskanaler fundet!")
        sys.exit(1)

    products = get_unpublished_products(publications)
    published = publish_to_all_channels(products, publications)

    # GitHub Actions output
    gh = os.environ.get('GITHUB_OUTPUT', '')
    if gh:
        with open(gh, 'a') as f:
            f.write(f"published_count={published}\n")
            f.write(f"checked_count={len(products)}\n")

except Exception as e:
    print(f"\n❌ FATAL: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
