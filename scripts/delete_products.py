import pandas as pd
import requests
import zipfile
import io
import json
import os
import sys
import time
from datetime import datetime

print("VidaXL Delete Processor - Automatisk (GitHub Actions)")
print("=" * 60)

# ============================================================
# KONFIGURATION FRA ENVIRONMENT VARIABLES
# ============================================================
FEED_URL = os.environ.get('FEED_URL', '')
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE', '')  # f.eks. din-butik.myshopify.com
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN', '')
DELETE_THRESHOLD = int(os.environ.get('DELETE_THRESHOLD', '1000'))

# Valider at vi har alt vi skal bruge
missing = []
if not FEED_URL:
    missing.append('FEED_URL')
if not SHOPIFY_STORE:
    missing.append('SHOPIFY_STORE')
if not SHOPIFY_ACCESS_TOKEN:
    missing.append('SHOPIFY_ACCESS_TOKEN')

if missing:
    print(f"❌ Manglende environment variables: {', '.join(missing)}")
    sys.exit(1)


def normalize_sku(sku):
    """Normaliser SKU - fjern trailing .0 fra float-konverteringer"""
    if pd.isna(sku):
        return ''
    return str(sku).strip().replace('.0', '')


def fetch_feed_data(url):
    """Hent og udpak produkt feed fra ZIP URL"""
    print(f"\n📥 Henter feed data fra URL...")
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    print(f"   Download: {len(response.content) / 1024 / 1024:.1f} MB")

    # Udpak ZIP
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise Exception("Ingen CSV fil fundet i ZIP")

        print(f"   Udpakker: {csv_files[0]}")
        with zf.open(csv_files[0]) as csv_file:
            df = pd.read_csv(csv_file, encoding='utf-8', on_bad_lines='skip')

    return df


def fetch_shopify_skus_graphql(store, token):
    """Hent alle variant SKUs fra Shopify via GraphQL Admin API (meget hurtigere end REST)"""
    print(f"\n📥 Henter Shopify produkter via GraphQL API...")

    skus = set()
    url = f"https://{store}/admin/api/2024-10/graphql.json"
    headers = {
        'X-Shopify-Access-Token': token,
        'Content-Type': 'application/json'
    }

    has_next_page = True
    cursor = None
    total_fetched = 0

    while has_next_page:
        # Byg query med eller uden cursor
        if cursor:
            query = '''
            {
                productVariants(first: 250, after: "%s") {
                    edges {
                        node {
                            sku
                        }
                        cursor
                    }
                    pageInfo {
                        hasNextPage
                    }
                }
            }
            ''' % cursor
        else:
            query = '''
            {
                productVariants(first: 250) {
                    edges {
                        node {
                            sku
                        }
                        cursor
                    }
                    pageInfo {
                        hasNextPage
                    }
                }
            }
            '''

        response = requests.post(url, headers=headers, json={'query': query}, timeout=60)
        response.raise_for_status()

        data = response.json()

        # Check for errors
        if 'errors' in data:
            print(f"   ⚠️ GraphQL fejl: {data['errors']}")
            # Check for throttling
            if any('Throttled' in str(e) for e in data['errors']):
                print("   ⏳ Rate limited - venter 2 sekunder...")
                time.sleep(2)
                continue
            raise Exception(f"GraphQL fejl: {data['errors']}")

        # Check for throttling via extensions
        extensions = data.get('extensions', {})
        cost = extensions.get('cost', {})
        throttle_status = cost.get('throttleStatus', {})
        currently_available = throttle_status.get('currentlyAvailable', 1000)

        # Hvis vi nærmer os rate limit, vent lidt
        if currently_available < 100:
            time.sleep(1)

        # Udtræk SKUs
        variants = data.get('data', {}).get('productVariants', {})
        edges = variants.get('edges', [])

        for edge in edges:
            sku = edge.get('node', {}).get('sku')
            if sku:
                skus.add(normalize_sku(sku))

        total_fetched += len(edges)

        # Pagination
        page_info = variants.get('pageInfo', {})
        has_next_page = page_info.get('hasNextPage', False)

        if has_next_page and edges:
            cursor = edges[-1].get('cursor')

        if total_fetched % 5000 == 0:
            print(f"   Hentet {total_fetched:,} varianter ({len(skus):,} unikke SKUs)...")

    print(f"✅ {len(skus):,} unikke SKUs hentet fra Shopify ({total_fetched:,} varianter total)")
    return skus


# ============================================================
# HOVEDPROCESSERING
# ============================================================

try:
    # 1. Hent data
    products = fetch_feed_data(FEED_URL)
    products['SKU'] = products['SKU'].apply(normalize_sku)
    print(f"✅ {len(products):,} produkter i feed")

    shopify_skus = fetch_shopify_skus_graphql(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN)

    # 2. Find SKUs der skal slettes
    print("\n🔍 Finder udgåede produkter...")
    current_skus = set(products['SKU'].unique())

    to_delete_skus = shopify_skus - current_skus

    print(f"\n📊 Resultat:")
    print(f"   - Produkter i Shopify: {len(shopify_skus):,}")
    print(f"   - Produkter i feed: {len(current_skus):,}")
    print(f"   - Produkter der skal slettes: {len(to_delete_skus):,}")

    # 3. Output til GitHub Actions
    delete_count = len(to_delete_skus)
    needs_approval = "true" if delete_count > DELETE_THRESHOLD else "false"

    # Skriv outputs til GITHUB_OUTPUT
    github_output = os.environ.get('GITHUB_OUTPUT', '')
    if github_output:
        with open(github_output, 'a') as f:
            f.write(f"delete_count={delete_count}\n")
            f.write(f"needs_approval={needs_approval}\n")
            f.write(f"feed_count={len(current_skus)}\n")
            f.write(f"shopify_count={len(shopify_skus)}\n")

    # 4. Generer slettefil
    if delete_count > 0:
        delete_df = pd.DataFrame({
            'Variant SKU': sorted(list(to_delete_skus)),
            'Variant Command': 'DELETE'
        })

        # Vis eksempler
        print(f"\n🗑️ Eksempel på produkter der slettes:")
        for sku in list(to_delete_skus)[:10]:
            print(f"   - {sku}")
        if delete_count > 10:
            print(f"   ... og {delete_count - 10} mere")

        # Threshold check
        if delete_count > DELETE_THRESHOLD:
            print(f"\n⚠️ THRESHOLD OVERSKREDET! {delete_count:,} > {DELETE_THRESHOLD:,}")
            print("   Slettefilen genereres, men kræver manuel godkendelse.")
        else:
            print(f"\n✅ Under threshold ({delete_count:,} ≤ {DELETE_THRESHOLD:,}) - kører automatisk")

        # Gem altid filen (approval-flowet styrer om den committes)
        output_path = os.path.join('output', 'matrixify_delete.csv')
        delete_df.to_csv(output_path, index=False, encoding='utf-8-sig', sep=';')
        print(f"💾 Slettefil gemt: {output_path}")

    else:
        print("\n✅ Ingen produkter skal slettes!")

        # Gem tom fil så Matrixify ikke fejler
        empty_df = pd.DataFrame({
            'Variant SKU': [],
            'Variant Command': []
        })
        output_path = os.path.join('output', 'matrixify_delete.csv')
        empty_df.to_csv(output_path, index=False, encoding='utf-8-sig', sep=';')
        print(f"💾 Tom fil gemt: {output_path}")

    print(f"\n✅ Færdig!")

except Exception as e:
    print(f"\n❌ FATAL FEJL: {e}")
    import traceback
    print(traceback.format_exc())
    sys.exit(1)
