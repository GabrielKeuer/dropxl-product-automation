"""Debug scrape af specifik vidaXL URL — vis HVAD vi finder af options."""
import sys, os, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from product_utils import scrape_vidaxl, fetch_variant_skus

URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.vidaxl.dk/e/vidaxl-tv-borde-2-stk.-med-led-lys-305x30x102-cm-hvid/8721012256002.html"

print(f"URL: {URL}\n")
result = scrape_vidaxl(URL)
print(f"master_pid: {result['master_pid']}")
print(f"success: {result['success']}")
print(f"options keys: {list(result['options'].keys())}")
print()
for attr_name, od in result['options'].items():
    print(f"  Option attr='{attr_name}' display_name='{od['display_name']}'")
    print(f"    {len(od['values'])} values:")
    for v in od['values'][:5]:
        print(f"      - {v['value']} → '{v['display']}'")
    if len(od['values']) > 5:
        print(f"      ... og {len(od['values']) - 5} flere")
    print()

# Hent variants for at se hvilke SKUs vi finder
if result['options']:
    print("Henter variant-SKUs...")
    variant_map = fetch_variant_skus(result['master_pid'], result['options'])
    print(f"\n{len(variant_map)} variants:")
    for sku, opts in list(variant_map.items())[:5]:
        print(f"  SKU {sku}: {opts}")
