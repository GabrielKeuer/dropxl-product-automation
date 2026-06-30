"""
Shared VidaXL pricing logic.

MASTER COPY: vidaxl-pris-lager/scripts/pricing.py
Mirrored to:  dropxl-product-automation/scripts/pricing.py

Loads tier-based pricing config from Supabase with hierarchical match:

  type-specifik (pricing_rules: vendor + product_type)
  > vendor-general (pricing_rules: vendor + product_type IS NULL)
  > global default (hub_settings.product_automation_pricing)  ← ultimate fallback

BACKWARD COMPAT: load_pricing_config() uden args returnerer global config
(samme adfaerd som foer Katalog Engine).

Config schema (identisk på alle tre niveauer):

    {
      "version": 1,
      "default_min_markup_after_discount": 1.65,
      "rounding": "ceil_50_minus_1",
      "tiers": [
        { "max_b2b": 300,  "markup": 2.65, "sale_discount_pct": 30,
          "sale_min_markup_override": null },
        ...
      ],
      "campaigns": []
    }

Tier match rule: first tier where max_b2b >= b2b. max_b2b=null means "infinity" (last tier).
"""

import hashlib
import math
import os
from datetime import datetime, timezone

DEFAULT_FALLBACK_MARKUP = 1.7
DEFAULT_MIN_MARKUP_AFTER_DISCOUNT = 1.65
DEFAULT_ROUNDING = "ceil_50_minus_1"
FX_EUR_DKK = 7.46
DEFAULT_VAT = 1.25


def _round_ceil_50_minus_1(price):
    """Rund altid OP til naermeste X49/X99 (50-trin minus 1)."""
    if price <= 0:
        return 0
    return int(math.ceil(price / 50) * 50 - 1)


def _round_nearest_50_minus_1(price):
    """Rund til NAERMESTE X49/X99 (50-trin minus 1)."""
    if price <= 0:
        return 0
    return int(((price + 25) // 50) * 50 - 1)


def _round_ceil_100_minus_1(price):
    """Rund altid OP til naermeste X99 (100-trin minus 1). Skip X49."""
    if price <= 0:
        return 0
    return int(math.ceil(price / 100) * 100 - 1)


def _round_nearest_100_minus_1(price):
    """Rund til NAERMESTE X99. Kan gaa op eller ned."""
    if price <= 0:
        return 0
    return int(((price + 50) // 100) * 100 - 1)


def _round_9(price):
    """Rund altid OP til naermeste tal der slutter paa 9 (10-trin minus 1).
    Bruges af Sollux (priser som 39, 439, 889, 1429 — ikke x49/x99)."""
    if price <= 0:
        return 0
    p = int(price)
    return p if p % 10 == 9 else ((p // 10) + 1) * 10 - 1


_ROUNDING_FUNCS = {
    "ceil_100_minus_1": _round_ceil_100_minus_1,
    "ceil_50_minus_1": _round_ceil_50_minus_1,
    "nearest_100_minus_1": _round_nearest_100_minus_1,
    "round_50_minus_1": _round_nearest_50_minus_1,
    "round_9": _round_9,
}


def _round_price(price, rounding=DEFAULT_ROUNDING):
    func = _ROUNDING_FUNCS.get(rounding)
    if func is None:
        raise ValueError(f"Unknown rounding rule: {rounding}")
    return func(price)


def normalize_sku(sku):
    if sku is None:
        return ""
    s = str(sku).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def assign_group(sku):
    """Stable A/B/C assignment via md5(sku) % 3. Same SKU -> same group, always."""
    s = normalize_sku(sku)
    if not s:
        return "A"
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return ["A", "B", "C"][int(h, 16) % 3]


def _select_tier(b2b, tiers):
    for tier in tiers:
        max_b2b = tier.get("max_b2b")
        if max_b2b is None:
            return tier
        if b2b <= float(max_b2b):
            return tier
    return tiers[-1]


def calculate_normal_price(b2b, config):
    """Normal selling price (no discount). Returns int or 0 on bad input."""
    try:
        b2b = float(b2b)
    except (TypeError, ValueError):
        return 0
    if b2b <= 0:
        return 0
    tiers = (config or {}).get("tiers") or []
    if not tiers:
        return _round_price(b2b * DEFAULT_FALLBACK_MARKUP)
    tier = _select_tier(b2b, tiers)
    rounding = (config or {}).get("rounding", DEFAULT_ROUNDING)
    return _round_price(b2b * float(tier["markup"]), rounding)


def calculate_sale_price(b2b, config):
    """Discounted sale price respecting min markup floor. Returns int or None if no discount applies."""
    try:
        b2b = float(b2b)
    except (TypeError, ValueError):
        return None
    if b2b <= 0:
        return None
    tiers = (config or {}).get("tiers") or []
    if not tiers:
        return None
    tier = _select_tier(b2b, tiers)
    discount_pct = float(tier.get("sale_discount_pct") or 0)
    if discount_pct <= 0:
        return None

    normal = calculate_normal_price(b2b, config)
    sale = _round_nearest_50_minus_1(normal * (1 - discount_pct / 100.0))

    min_markup = tier.get("sale_min_markup_override")
    if min_markup is None:
        min_markup = (config or {}).get(
            "default_min_markup_after_discount", DEFAULT_MIN_MARKUP_AFTER_DISCOUNT
        )
    min_markup = float(min_markup)
    while sale > 0 and sale / b2b < min_markup:
        sale = _round_nearest_50_minus_1(sale + 50)
    return sale


# =============================================================================
# FICTIVE-DISCOUNT MODE (Kayoom-stil) — Omnibus OFF
# =============================================================================
# Kunden betaler ALTID: cost × fixed_markup (rundet op til x49/x99).
# Førpris (compareAt) = sale / (1 - fiktiv rabat), hvor rabatten vælges
# DETERMINISTISK pr. produkt (seed) blandt fictive_discounts (fx [15,20,25,30]).
# Altid på tilbud. IKKE Omnibus-lovlig (førprisen er fiktiv/marketing).

def _pick_fictive_discount(seed, discounts):
    """Vælg fiktiv rabat deterministisk pr. produkt (samme seed -> samme rabat)."""
    if not discounts:
        return 0
    h = int(hashlib.md5(str(seed).encode("utf-8")).hexdigest(), 16)
    return float(discounts[h % len(discounts)])


def calculate_fictive_prices(cost, config, seed):
    """Fictive-discount: returnerer {normal_price, sale_price, discount_pct}.

    normal_price = den OVERSTREGEDE førpris (Shopify compareAtPrice)
    sale_price   = det kunden BETALER (Shopify price) = cost × fixed_markup
    seed         = produkt-identifikator (model/handle) så alle varianter af et
                   produkt får SAMME rabat.
    """
    try:
        cost = float(cost)
    except (TypeError, ValueError):
        return {"normal_price": 0, "sale_price": 0, "discount_pct": 0}
    if cost <= 0:
        return {"normal_price": 0, "sale_price": 0, "discount_pct": 0}
    cfg = config or {}
    markup = float(cfg.get("fixed_markup", DEFAULT_FALLBACK_MARKUP))
    rounding = cfg.get("rounding", DEFAULT_ROUNDING)
    discounts = cfg.get("fictive_discounts") or []
    surcharge = float(cfg.get("flat_surcharge", 0) or 0)    # fast tillæg efter markup (Sollux-pærer +10)

    sale = _round_price(cost * markup, rounding)            # det kunden betaler
    if surcharge:
        sale = int(sale + surcharge)
    disc = _pick_fictive_discount(seed, discounts)
    if disc <= 0:
        return {"normal_price": sale, "sale_price": sale, "discount_pct": 0}
    compare = _round_nearest_50_minus_1(sale / (1 - disc / 100.0))   # førpris (nærmeste x49/x99)
    if compare <= sale:                                    # sikkerhed: førpris altid > udsalg
        compare = _round_ceil_50_minus_1(sale / (1 - disc / 100.0))
    return {"normal_price": compare, "sale_price": sale, "discount_pct": disc}


def get_active_campaign(now=None, config=None):
    if not config:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    for c in config.get("campaigns") or []:
        if not c.get("enabled"):
            continue
        try:
            start = datetime.fromisoformat(c["start"])
            end = datetime.fromisoformat(c["end"])
        except (KeyError, ValueError, TypeError):
            continue
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        if start <= now <= end:
            return c
    return None


def calculate_prices(b2b, config, now=None, seed=None):
    """One-shot: returns {normal_price, sale_price, campaign_id, discount_pct}.

    Mode-bevidst (config['mode']):
      - 'real_discount' (default/manglende): eksisterende tier-model, Omnibus-lovlig.
      - 'fictive_discount': fast markup + fiktiv førpris (Kayoom-stil), Omnibus OFF.
        seed = produkt-id (alle varianter af et produkt får samme fiktive rabat).
    """
    mode = (config or {}).get("mode", "real_discount")
    if mode == "fictive_discount":
        r = calculate_fictive_prices(b2b, config, seed if seed is not None else b2b)
        return {"normal_price": r["normal_price"], "sale_price": r["sale_price"],
                "campaign_id": None, "discount_pct": r["discount_pct"]}

    campaign = get_active_campaign(now=now, config=config)
    normal = calculate_normal_price(b2b, config)

    if campaign and campaign.get("mode") == "flat_discount_pct":
        try:
            b2b_f = float(b2b)
        except (TypeError, ValueError):
            b2b_f = 0
        rounding = (config or {}).get("rounding", DEFAULT_ROUNDING)
        sale = _round_price(normal * (1 - float(campaign["value"]) / 100.0), rounding)
        floor = float(campaign.get("min_markup_floor", 1.5))
        while b2b_f > 0 and sale > 0 and sale / b2b_f < floor:
            sale += 50
            sale = _round_price(sale, rounding)
        return {"normal_price": normal, "sale_price": sale, "campaign_id": campaign.get("id")}

    return {
        "normal_price": normal,
        "sale_price": calculate_sale_price(b2b, config),
        "campaign_id": None,
    }


def resolve_variant_pricing(b2b, config, seed=None, on_sale=False):
    """Mode-bevidst: returnér (price, compare_at_price) til en variant.

    real_discount:
      - on_sale + gyldig udsalgspris  -> (sale, normal)   [normal vises overstreget]
      - ellers                        -> (normal, None)    [intet tilbud]
    fictive_discount (altid på tilbud):
      - (sale, normal)   [kunden betaler sale; normal = fiktiv førpris]

    Bruges af alle pris-stier (create, sync, bulk) så én config styrer alt.
    """
    res = calculate_prices(b2b, config, seed=seed)
    normal = res.get("normal_price") or 0
    sale = res.get("sale_price")
    mode = (config or {}).get("mode", "real_discount")
    if mode == "fictive_discount":
        if not sale or not normal or normal <= sale:
            return (normal or sale or 0), None
        return sale, normal
    # real_discount
    if on_sale and sale and normal and sale < normal:
        return sale, normal
    return normal, None


# =============================================================================
# FRAGT + MARGIN (kun til visning/avance — IKKE i markup-basen)
# =============================================================================
# shipping-deskriptor i config (pr. vendor):
#   { "in_cost_basis": bool,            # true = fragt allerede i cost (Kayoom) -> 0 i margin
#     "currency": "EUR"|"DKK",
#     "model": "free" | "tiered" | "fixed_free_above" | "fixed",
#     "tiers": [{"min": 0, "fee": 19.90}, ...],   # tiered (Benuta), sorteret asc på min
#     "fee": 7.0, "free_above": 120.0 }           # fixed_free_above (Sollux)

def calc_shipping_dkk(cost_dkk, shipping_config):
    """Vendor-fragt i DKK for et produkt med given cost (DKK). 0 hvis fragt
    allerede er i cost-basen (Kayoom) eller gratis (vidaXL). Trin slås op ud fra
    indkøbsprisen i fragt-modellens valuta (Benuta/Sollux er EUR-baseret)."""
    sc = shipping_config or {}
    if sc.get("in_cost_basis") or sc.get("model") in (None, "free"):
        return 0.0
    try:
        cost_dkk = float(cost_dkk)
    except (TypeError, ValueError):
        return 0.0
    cur = sc.get("currency", "EUR")
    cost_cur = cost_dkk / FX_EUR_DKK if cur == "EUR" else cost_dkk
    model = sc.get("model")
    fee = 0.0
    if model == "fixed_free_above":
        if cost_cur < float(sc.get("free_above", 0)):
            fee = float(sc.get("fee", 0))
    elif model == "tiered":
        for t in sorted(sc.get("tiers") or [], key=lambda x: float(x.get("min", 0))):
            if cost_cur >= float(t.get("min", 0)):
                fee = float(t.get("fee", 0))
    elif model == "fixed":
        fee = float(sc.get("fee", 0))
    return fee * FX_EUR_DKK if cur == "EUR" else fee


def calc_margin(price, cost_dkk, shipping_config=None, vat=DEFAULT_VAT):
    """Ex-moms avance i DKK: (pris/vat) - cost - vendor-fragt. Til hub-pris-eksempler."""
    try:
        price = float(price); cost_dkk = float(cost_dkk)
    except (TypeError, ValueError):
        return 0.0
    ship = calc_shipping_dkk(cost_dkk, shipping_config)
    return (price / vat) - cost_dkk - ship


# =============================================================================
# CONFIG LOADING (med Katalog Engine hierarki)
# =============================================================================

def _get_supabase_client():
    """Lazy create Supabase client from env."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        return None
    try:
        from supabase import create_client
        return create_client(url, key)
    except Exception as e:
        print(f"[pricing] Supabase client init failed: {e}")
        return None


def _is_valid_config(cfg):
    """En config er gyldig hvis den er tier-baseret (real_discount) ELLER
    fictive_discount med fixed_markup."""
    if not isinstance(cfg, dict):
        return False
    if cfg.get("mode") == "fictive_discount":
        return cfg.get("fixed_markup") is not None
    return bool(cfg.get("tiers"))


def _try_load_pricing_rule(client, vendor, product_type):
    """Forsoeg at hente en specifik pricing_rules-row.

    product_type=None -> vendor-general regel
    product_type=str  -> type-specifik override
    """
    try:
        query = (
            client.table("pricing_rules")
            .select("config")
            .eq("vendor", vendor)
            .eq("enabled", True)
        )
        if product_type is None:
            query = query.is_("product_type", "null")
        else:
            query = query.eq("product_type", product_type)
        res = query.limit(1).execute()
        if res.data and len(res.data) > 0:
            cfg = res.data[0]["config"]
            if _is_valid_config(cfg):
                return cfg
    except Exception as e:
        print(f"[pricing] rule load failed (vendor={vendor}, type={product_type}): {e}")
    return None


def _load_global_pricing_config(client):
    """Fallback til hub_settings.product_automation_pricing."""
    try:
        res = (
            client.table("hub_settings")
            .select("value")
            .eq("key", "product_automation_pricing")
            .execute()
        )
        if res.data:
            cfg = res.data[0]["value"]
            if _is_valid_config(cfg):
                return cfg
    except Exception as e:
        print(f"[pricing] global config load failed: {e}")
    return None


def load_pricing_config(supabase_client=None, vendor=None, product_type=None):
    """Load pricing config with hierarchical match.

    Order:
      1. pricing_rules: vendor + product_type   (type-specifik override)
      2. pricing_rules: vendor + NULL           (vendor-general)
      3. hub_settings.product_automation_pricing (global default)

    Backward compat:
      load_pricing_config() (uden args) returnerer global default,
      identisk med adfaerd foer Katalog Engine.

    Returns: dict | None
    """
    client = supabase_client or _get_supabase_client()
    if client is None:
        return None

    if vendor:
        # 1. Forsøg type-specifik
        if product_type:
            cfg = _try_load_pricing_rule(client, vendor, product_type)
            if cfg:
                return cfg
        # 2. Forsøg vendor-general
        cfg = _try_load_pricing_rule(client, vendor, None)
        if cfg:
            return cfg

    # 3. Global fallback
    return _load_global_pricing_config(client)


# -----------------------------------------------------------------------------
# Self-test
# -----------------------------------------------------------------------------

_TEST_CONFIG = {
    "version": 1,
    "default_min_markup_after_discount": 1.65,
    "rounding": "ceil_50_minus_1",
    "tiers": [
        {"max_b2b": 300, "markup": 2.65, "sale_discount_pct": 30, "sale_min_markup_override": None},
        {"max_b2b": 500, "markup": 2.45, "sale_discount_pct": 25, "sale_min_markup_override": None},
        {"max_b2b": 1000, "markup": 2.20, "sale_discount_pct": 20, "sale_min_markup_override": None},
        {"max_b2b": 1500, "markup": 2.00, "sale_discount_pct": 15, "sale_min_markup_override": None},
        {"max_b2b": None, "markup": 1.89, "sale_discount_pct": 10, "sale_min_markup_override": 1.60},
    ],
    "campaigns": [],
}

_TEST_FICTIVE = {
    "mode": "fictive_discount",
    "fixed_markup": 2.0,
    "fictive_discounts": [15, 20, 25, 30],
    "rounding": "ceil_50_minus_1",
}

if __name__ == "__main__":
    print("=== real_discount (backward-compat, skal være uændret) ===")
    print("normal(100) =", calculate_normal_price(100, _TEST_CONFIG), "expected 299")
    print("sale(100)   =", calculate_sale_price(100, _TEST_CONFIG), "expected ~199")
    print("normal(1000)=", calculate_normal_price(1000, _TEST_CONFIG))
    print("group('837016') =", assign_group("837016"))
    # config UDEN mode skal opføre sig som real_discount
    r = calculate_prices(100, _TEST_CONFIG)
    assert r["normal_price"] == 299, r
    print("calculate_prices(100) uden mode -> real_discount OK:", r)

    print("\n=== fictive_discount (Kayoom-stil) ===")
    for cost, seed in [(275, "Earthquake 225"), (195, "Surfacer 225"), (640, "Amora 263")]:
        r = calculate_fictive_prices(cost, _TEST_FICTIVE, seed)
        print(f"  cost={cost} seed={seed!r}: udsalg={r['sale_price']} førpris={r['normal_price']} rabat={r['discount_pct']:.0f}%")
    # via calculate_prices med mode
    rp = calculate_prices(275, _TEST_FICTIVE, seed="Earthquake 225")
    assert rp["sale_price"] == 549, rp
    print("calculate_prices(275, fictive, seed=Earthquake 225) -> udsalg 549 OK:", rp)

    print("\n=== resolve_variant_pricing (price, compareAt) ===")
    # real_discount: ikke på tilbud -> (normal, None); på tilbud -> (sale, normal)
    assert resolve_variant_pricing(100, _TEST_CONFIG, on_sale=False) == (299, None)
    assert resolve_variant_pricing(100, _TEST_CONFIG, on_sale=True) == (199, 299)
    # fictive: altid på tilbud -> (sale, før)
    assert resolve_variant_pricing(275, _TEST_FICTIVE, seed="Earthquake 225") == (549, 799)
    print("real(100) off ->", resolve_variant_pricing(100, _TEST_CONFIG, on_sale=False))
    print("real(100) on  ->", resolve_variant_pricing(100, _TEST_CONFIG, on_sale=True))
    print("fictive(275)  ->", resolve_variant_pricing(275, _TEST_FICTIVE, seed="Earthquake 225"))

    print("\n=== fragt-opslag (margin) ===")
    benuta_ship = {"currency": "EUR", "model": "tiered", "tiers": [
        {"min": 0, "fee": 19.90}, {"min": 100, "fee": 24.90}, {"min": 1000, "fee": 49.90},
        {"min": 2000, "fee": 99.90}, {"min": 5000, "fee": 199.90}]}
    sollux_ship = {"currency": "EUR", "model": "fixed_free_above", "fee": 7.0, "free_above": 120.0}
    # Benuta: 80 EUR cost -> 19.90 EUR fragt; 150 -> 24.90; 1500 -> 49.90
    assert round(calc_shipping_dkk(80 * FX_EUR_DKK, benuta_ship) / FX_EUR_DKK, 2) == 19.90
    assert round(calc_shipping_dkk(150 * FX_EUR_DKK, benuta_ship) / FX_EUR_DKK, 2) == 24.90
    assert round(calc_shipping_dkk(1500 * FX_EUR_DKK, benuta_ship) / FX_EUR_DKK, 2) == 49.90
    # Sollux: 50 EUR -> 7; 150 EUR -> gratis (0)
    assert round(calc_shipping_dkk(50 * FX_EUR_DKK, sollux_ship) / FX_EUR_DKK, 2) == 7.0
    assert calc_shipping_dkk(150 * FX_EUR_DKK, sollux_ship) == 0.0
    # Kayoom: fragt i cost-basen -> 0
    assert calc_shipping_dkk(500, {"in_cost_basis": True}) == 0.0
    print("Benuta 80€->fragt", round(calc_shipping_dkk(80*FX_EUR_DKK, benuta_ship)/FX_EUR_DKK,2), "€ | 150€->", round(calc_shipping_dkk(150*FX_EUR_DKK, benuta_ship)/FX_EUR_DKK,2), "€")
    print("Sollux 50€->", round(calc_shipping_dkk(50*FX_EUR_DKK, sollux_ship)/FX_EUR_DKK,2), "€ | 150€->", round(calc_shipping_dkk(150*FX_EUR_DKK, sollux_ship)/FX_EUR_DKK,2), "€")
    print("\nALLE ASSERTS OK ✓")
