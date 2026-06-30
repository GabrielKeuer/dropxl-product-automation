"""Merge: tilføj NYE farve-varianter til EKSISTERENDE Sollux-produkter.

Genbruger dropxl's battle-testede merge-motor (create_products_v2.call_variants_merge):
 - optionValues bygges fra produktets EGNE option-navne (kan ikke byttes om)
 - kombo-tjek undgår VARIANT_ALREADY_EXISTS
 - SKU i inventoryItem.sku, availableQuantity, media-upload + farve-fallback
 - Case A (Title→Farve-konvertering) håndteres atomisk via productSet

Priser/struktur fra Sollux-laget (create_sollux_products): hub-pricing (fictive),
farve fra Farve-kolonnen, billed-rotation, metafelter custom.sku/produktinfo/variantbilleder.

Brug:
  python merge_sollux_variants.py                      # dry-run: list merge-grupper
  python merge_sollux_variants.py --test-sku SL.XXXX   # opret KUN den ene nye SKU (live)
  python merge_sollux_variants.py --apply              # alle merge-grupper (live)
Env: SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_KEY
"""
import os, sys, time, json, argparse
import pandas as pd, requests, warnings
warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import create_sollux_products as scp     # Sollux feed/helpers/pricing
import create_products_v2 as cp          # battle-tested merge-motor + VariantSpec/MergeSpec

SHOP = os.environ['SHOPIFY_STORE']; TOK = os.environ['SHOPIFY_ACCESS_TOKEN']
URL = f"https://{SHOP}/admin/api/2024-10/graphql.json"; H = {"X-Shopify-Access-Token": TOK, "Content-Type": "application/json"}
EUR = 7.45

def gql(q, v=None):
    return requests.post(URL, headers=H, json={"query": q, "variables": v or {}}, timeout=60).json()

def sku_to_handle_map():
    """Map: Sollux variant-SKU -> produkt-handle (+ handle->option-navne via senere opslag)."""
    m = {}; q = """query($c:String){products(first:80,query:"vendor:Sollux",after:$c){pageInfo{hasNextPage endCursor}edges{node{handle variants(first:40){edges{node{sku}}}}}}}"""
    cur = None
    while True:
        r = gql(q, {"c": cur}); pr = r["data"]["products"]
        for e in pr["edges"]:
            for v in e["node"]["variants"]["edges"]:
                if v["node"]["sku"]: m[v["node"]["sku"].strip()] = e["node"]["handle"]
        if not pr["pageInfo"]["hasNextPage"]: break
        cur = pr["pageInfo"]["endCursor"]; time.sleep(0.2)
    return m

def build_variantspec(row, sku, color, is_variant_group, cfg, handle_seed):
    gross = float(row['Gross retail price (EUR)'])
    pc = gross / 1.20 * 0.55 * EUR
    price, compare = scp.pricing.resolve_variant_pricing(pc, cfg, seed=handle_seed)
    cost = int(scp._cost_map_global[sku] * EUR) if hasattr(scp, '_cost_map_global') else 0
    try: w = int(float(str(row.get('Nettovægt (kg)', '0')).replace(',', '.')) * 1000)
    except: w = 0
    imgs = scp.images_for(row, is_bulb=False)
    mf = [{"namespace": "custom", "key": "sku", "type": "single_line_text_field", "value": sku}]
    pi = scp.produktinfo_html(row.get('Long description HTML - benefits'), row.get('Short description HTML'))
    if pi: mf.append({"namespace": "custom", "key": "produktinfo", "type": "multi_line_text_field", "value": pi})
    if imgs: mf.append({"namespace": "custom", "key": "variantbilleder", "type": "list.single_line_text_field", "value": json.dumps(imgs)})
    return cp.VariantSpec(sku=sku, price=int(price), cost=float(cost), weight_grams=w,
        inventory_quantity=int(scp._stock_map_global.get(sku, 0)), barcode=str(row.get('EAN') or ''),
        compare_at_price=int(compare) if compare else None,
        option_values=[("Farve", color)] if color else [], image_url=imgs[0] if imgs else None, metafields=mf)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--test-sku', default=None)
    args = ap.parse_args()

    d, cost_map, stock_map = scp.load_feed()
    scp._cost_map_global = cost_map; scp._stock_map_global = stock_map
    lamp_cfg, bulb_cfg, _ = scp.load_sollux_configs()
    s2h = sku_to_handle_map()
    existing_skus = set(s2h.keys())

    d['Product name'] = d['Product name'].apply(lambda x: scp.apply_fixes(x, scp.TEXT_FIXES))
    d['_color'] = d['Farve'].apply(scp.norm_color)
    d['_base'] = d.apply(lambda r: scp.base_from_name_color(r['Product name'], r['_color']), axis=1)

    groups = []  # (handle, ptype, new_rows, existing_rows_in_group)
    for base, grp in d.groupby('_base'):
        skus = [str(s).strip() for s in grp['SKU']]
        new = [s for s in skus if s not in existing_skus]
        old = [s for s in skus if s in existing_skus]
        if not new or not old: continue                      # kun ægte merge
        grp = grp[grp['SKU'].apply(lambda s: str(s).strip() in cost_map)]  # skal have kostpris
        new = [s for s in new if s in set(str(x).strip() for x in grp['SKU'])]
        if not new: continue
        handle = s2h.get(old[0])
        if not handle: continue
        ptype, _ = scp.map_type(grp.iloc[0].get('Kategori'))
        if ptype == 'Lyskilde': continue                     # pærer merges ikke (ingen farve)
        groups.append((handle, grp, new, old))

    print(f"Merge-grupper: {len(groups)} | nye varianter i alt: {sum(len(g[2]) for g in groups)}")
    if args.test_sku:
        groups = [g for g in groups if args.test_sku in g[2]]
        print(f"TEST: kun gruppe med SKU {args.test_sku} → {len(groups)} gruppe")

    if not args.apply and not args.test_sku:
        for h, grp, new, old in groups[:15]:
            print(f"  {h}: +{len(new)} nye farver {new} (eksist. {len(old)})")
        print("\nDRY-RUN. Brug --test-sku SL.XXXX (én) eller --apply (alle).")
        return

    loc = cp.get_primary_location_id()
    for h, grp, new, old in groups:
        cur_opts = cp.fetch_product_options(SHOP, TOK, h)
        new_variants, ex_var_opts = [], {}
        for _, row in grp.iterrows():
            sku = str(row['SKU']).strip(); color = row['_color']
            if args.test_sku and sku != args.test_sku:
                if sku in old:  # behold eksisterende mapping til Case A
                    ex_var_opts[sku] = [("Farve", color)] if color else []
                continue
            if sku in new:
                new_variants.append(build_variantspec(row, sku, color, True, lamp_cfg, h))
            else:
                ex_var_opts[sku] = [("Farve", color)] if color else []
        if not new_variants: continue
        has_farve = ("Farve" in cur_opts)
        try:
            if has_farve:
                # SIMPEL: produktet har allerede Farve → bulkCreate nye farve-varianter
                merge = cp.MergeSpec(existing_handle=h, options_to_add=[],
                                     existing_skus=old, new_variants=new_variants, existing_variant_options=ex_var_opts)
                res = cp.call_variants_merge(merge, loc)
            else:
                # KONVERTERING: single-Title → erstat med Farve (full_options=["Farve"], IKKE Title+Farve).
                # Eksisterende variant(er) får deres farve via existing_variant_options.
                pid = cp.find_product_by_handle(h)
                merge = cp.MergeSpec(existing_handle=h, options_to_add=["Farve"],
                                     existing_skus=old, new_variants=new_variants, existing_variant_options=ex_var_opts)
                res = cp._call_merge_via_productset(merge, pid, ["Farve"], loc)
            errs = res.get('userErrors') or []
            made = res.get('productVariants') or []
            print(f"  {h} [{'Farve' if has_farve else 'Title→Farve'}]: {'OK' if not errs else 'FEJL'} +{len(made)} {[v.get('sku') for v in made]}" + (f" {errs}" if errs else ""))
        except Exception as e:
            print(f"  {h}: EXCEPTION {e}")
        time.sleep(0.5)

if __name__ == '__main__':
    main()
