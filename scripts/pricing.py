"""
Shared VidaXL pricing logic.

MASTER COPY: vidaxl-pris-lager/scripts/pricing.py
Mirrored to:  dropxl-product-automation/scripts/pricing.py

Loads tier-based pricing config from Supabase (hub_settings.product_automation_pricing)
and provides:

  - calculate_normal_price(b2b, config)
  - calculate_sale_price(b2b, config)
  - assign_group(sku)                  -> 'A' | 'B' | 'C'   (stable hash)
  - get_active_campaign(now, config)
  - calculate_prices(b2b, config, now) -> {normal_price, sale_price, campaign}
  - load_pricing_config(supabase=None) -> dict | None

Config schema (in hub_settings.product_automation_pricing.value):

    {
      "version": 1,
      "default_min_markup_after_discount": 1.65,
      "rounding": "ceil_50_minus_1",
      "tiers": [
        { "max_b2b": 300,  "markup": 2.65, "sale_discount_pct": 30,
          "sale_min_markup_override": null },
        ...,
        { "max_b2b": null, "markup": 1.89, "sale_discount_pct": 10,
          "sale_min_markup_override": 1.60 }
      ],
      "campaigns": [
        { "id": "...", "enabled": false, "start": ISO8601, "end": ISO8601,
          "scope": "all_vidaxl", "mode": "flat_discount_pct", "value": 25,
          "min_markup_floor": 1.50 }
      ]
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
    if price <= 0:
        return 0
    return int(math.ceil(price / 50) * 50 - 1)


def _round_nearest_50_minus_1(price):
    if price <= 0:
        return 0
    return int(((price + 25) // 50) * 50 - 1)


_ROUNDING_FUNCS = {
    "ceil_50_minus_1": _round_ceil_50_minus_1,
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
    """Discounted sale price respecting min markup floor. Returns int or None if no discount applies.

    Rounding strategy: normal price uses "ceil_50_minus_1" (config-driven, X99 psychological).
    Sale price uses "round_50_minus_1" (round to nearest X49/X99) so the actual realised
    discount stays close to the target instead of always rounding up.

    The min-markup field is a *floor / bump-trigger*, not a target. Real markup will
    typically land at floor..floor+0.10 due to rounding granularity (50 kr).
    Target markup after discount is 1.70x; floor is 1.65x which catches results that
    drift more than ~0.05 below target due to rounding."""
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
    """One-shot: returns {normal_price, sale_price, campaign_id}.

    If a campaign is active and applies, sale_price is overridden by campaign rules.
    """
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


def load_pricing_config(supabase_client=None):
    """Load product_automation_pricing from Supabase. Returns dict or None on failure.

    If supabase_client is None, lazy-creates one from SUPABASE_URL + SUPABASE_SERVICE_KEY.
    """
    if supabase_client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not url or not key:
            return None
        try:
            from supabase import create_client
            supabase_client = create_client(url, key)
        except Exception as e:
            print(f"[pricing] Supabase client init failed: {e}")
            return None
    try:
        res = (
            supabase_client.table("hub_settings")
            .select("value")
            .eq("key", "product_automation_pricing")
            .execute()
        )
        if not res.data:
            return None
        cfg = res.data[0]["value"]
        if not isinstance(cfg, dict) or not cfg.get("tiers"):
            return None
        return cfg
    except Exception as e:
        print(f"[pricing] load failed: {e}")
        return None


# -----------------------------------------------------------------------------
# Self-test (run directly: python pricing.py)
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
        {"max_b2b": None, "markup": 1.89, "sale_discount_pct": 10, "sale_min_markup_override": None},
    ],
    "campaigns": [],
}


def _self_test():
    print("=== calculate_normal_price / calculate_sale_price ===")
    print(f"{'B2B':>6} | {'Tier':>4} | {'Normal':>6} | {'Sale':>6} | {'Disc%':>5} | {'M_normal':>8} | {'M_sale':>6}")
    print("-" * 70)
    for b2b in [50, 100, 200, 300, 301, 400, 500, 800, 1000, 1200, 1500, 1501, 2000, 5000]:
        normal = calculate_normal_price(b2b, _TEST_CONFIG)
        sale = calculate_sale_price(b2b, _TEST_CONFIG)
        tier = _select_tier(b2b, _TEST_CONFIG["tiers"])
        tier_label = tier.get("max_b2b") if tier.get("max_b2b") is not None else "inf"
        disc = (1 - sale / normal) * 100 if sale and normal else 0
        m_n = normal / b2b
        m_s = sale / b2b if sale else 0
        print(f"{b2b:>6} | {str(tier_label):>4} | {normal:>6} | {sale or '-':>6} | {disc:>4.1f}% | {m_n:>8.3f} | {m_s:>6.3f}")

    print()
    print("=== assign_group distribution (1000 sample SKUs) ===")
    counts = {"A": 0, "B": 0, "C": 0}
    for i in range(1000):
        counts[assign_group(f"SKU-{i:05d}")] += 1
    print(f"A: {counts['A']}, B: {counts['B']}, C: {counts['C']}")

    print()
    print("=== Stability check (same SKU = same group) ===")
    for sku in ["403659", "279890.0", "  279890  ", "abc-xyz"]:
        print(f"  {sku!r:>20} -> {assign_group(sku)}")


if __name__ == "__main__":
    _self_test()
