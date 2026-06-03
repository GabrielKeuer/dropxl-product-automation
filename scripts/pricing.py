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


_ROUNDING_FUNCS = {
    "ceil_100_minus_1": _round_ceil_100_minus_1,
    "ceil_50_minus_1": _round_ceil_50_minus_1,
    "nearest_100_minus_1": _round_nearest_100_minus_1,
    "round_50_minus_1": _round_nearest_50_minus_1,
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


def calculate_prices(b2b, config, now=None):
    """One-shot: returns {normal_price, sale_price, campaign_id}."""
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
            if isinstance(cfg, dict) and cfg.get("tiers"):
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
            if isinstance(cfg, dict) and cfg.get("tiers"):
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

if __name__ == "__main__":
    print("normal(100) =", calculate_normal_price(100, _TEST_CONFIG), "expected 299 (100 * 2.65 rounded ceil_50_minus_1)")
    print("sale(100)   =", calculate_sale_price(100, _TEST_CONFIG), "expected ~199 (after 30% discount, floor-checked)")
    print("normal(1000)=", calculate_normal_price(1000, _TEST_CONFIG))
    print("group('837016') =", assign_group("837016"))
