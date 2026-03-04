import pandas as pd
import requests
import zipfile
import io
import json
import os
import sys
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


def fetch_shopify_skus(store, token):
    """Hent alle variant SKUs fra Shopify via REST Admin API"""
    print(f"\n📥 Henter Shopify produkter via API...")

    skus = set()
    base_url = f"https://{store}/admin/api/2024-10/products.json"
    headers = {
        'X-Shopify-Access-Token': token,
        'Content-Type': 'application/json'
    }

    params = {
        'limit': 250,
        'fields': 'variants'
    }

    page = 1
    url = base_url

    while url:
        response = requests.get(url, headers=headers, params=params if page == 1 else None, timeout=60)
        response.raise_for_status()

        data = response.json()
        products = data.get('products', [])

        for product in products:
            for variant in product.get('variants', []):
                sku = variant.get('sku')
                if sku:
                    skus.add(normalize_sku(sku))

        # Shopify pagination via Link header
        link_header = response.headers.get('Link', '')
        url = None
        if 'rel="next"' in link_header:
            for part in link_header.split(','):
                if 'rel="next"' in part:
                    url = part.split('<')[1].split('>')[0]
                    break

        page += 1
        if page % 10 == 0:
            print(f"   Hentet {len(skus):,} SKUs ({page} sider)...")

    print(f"✅ {len(skus):,} SKUs hentet fra Shopify")
    return skus


# ============================================================
# HOVEDPROCESSERING
# ============================================================

try:
    # 1. Hent data
    products = fetch_feed_data(FEED_URL)
    products['SKU'] = products['SKU'].apply(normalize_sku)
    print(f"✅ {len(products):,} produkter i feed")

    shopify_skus = fetch_shopify_skus(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN)

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
