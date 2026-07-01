"""PILOT: kan vi bygge variant-mapping via scraping — og hvor dyrt?

Tester 3 ting på et udsnit af feedet (read-only, rører intet):
  1. Udtrækkes master_pid rent fra produktsiden?
  2. AFGØRENDE: ligger ALLE søster-SKUs i side-HTML'en?  → afgør ~30k vs ~150k scrapes
     (ground truth = fetch_variant_skus, som spørger vidaXL pr. kombination)
  3. Hastighed + blok-adfærd (non-200)

Skriver output/pilot_mapping.json + printer klar konklusion.
"""
from __future__ import annotations
import json, os, re, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests
from product_utils import (fetch_feed, scrape_vidaxl, fetch_variant_skus,
                           count_combinations, normalize_sku, validate_url, BROWSER_HEADERS)

FEED_URL = os.environ["FEED_URL"]
N = int(os.environ.get("PILOT_N", "120"))          # antal produkter til master_pid/tempo/blok
GT = int(os.environ.get("PILOT_GT", "12"))         # antal FLER-variant til ground-truth SKU-test
PID_RE = re.compile(r"pid=([A-Z]\d+)")
PID_RE2 = re.compile(r"dwvar_([A-Z]\d+)_")

def master_pid(html):
    m = PID_RE.search(html) or PID_RE2.search(html)
    return m.group(1) if m else None

def main():
    feed = fetch_feed(FEED_URL)
    print(f"📦 feed: {len(feed)} rækker, kolonner: {list(feed.columns)[:12]}...")
    # spred udsnittet ud over feedet + kræv gyldig Link
    step = max(1, len(feed) // (N * 4))
    sample = []
    for _, r in feed.iloc[::step].iterrows():
        if validate_url(str(r.get("Link", ""))):
            sample.append(r)
        if len(sample) >= N:
            break
    print(f"🔬 tester {len(sample)} produkter\n")

    pid_ok = 0; blocks = 0; times = []; htmls = {}
    for i, r in enumerate(sample):
        sku = normalize_sku(r["SKU"]); url = str(r["Link"])
        t0 = time.time()
        try:
            resp = requests.get(url, headers=BROWSER_HEADERS, timeout=30)
        except Exception as e:
            blocks += 1; print(f"  [{i+1}] {sku} FEJL {str(e)[:60]}"); continue
        dt = time.time() - t0; times.append(dt)
        if resp.status_code != 200:
            blocks += 1; print(f"  [{i+1}] {sku} HTTP {resp.status_code}"); continue
        pid = master_pid(resp.text)
        if pid: pid_ok += 1
        htmls[sku] = resp.text
        if (i + 1) % 25 == 0: print(f"  …{i+1}/{len(sample)}  (pid-ok={pid_ok}, blok={blocks})")
        time.sleep(0.4)

    # === AFGØRENDE TEST: ligger søster-SKUs i HTML'en? ===
    print("\n=== Ground-truth: søster-SKUs i side-HTML? ===")
    gt_results = []
    checked = 0
    for r in sample:
        if checked >= GT: break
        sku = normalize_sku(r["SKU"]); url = str(r["Link"])
        if sku not in htmls: continue
        sc = scrape_vidaxl(url)
        if not sc.get("success") or not sc.get("master_pid") or not sc.get("options"):
            continue
        combos = count_combinations(sc["options"])
        if combos < 2:
            continue  # kun fler-variant er interessant
        vmap = fetch_variant_skus(sc["master_pid"], sc["options"])  # ground truth søster-SKUs
        sibs = list(vmap.keys())
        if not sibs:
            continue
        html = htmls[sku]
        present = sum(1 for s in sibs if s in html)
        ratio = present / len(sibs)
        gt_results.append({"sku": sku, "master_pid": sc["master_pid"], "combos": combos,
                           "siblings": len(sibs), "in_html": present, "ratio": round(ratio, 2)})
        print(f"  {sku}: {len(sibs)} søster-SKUs, {present} i HTML  → {ratio:.0%}")
        checked += 1
        time.sleep(0.4)

    avg_ratio = sum(g["ratio"] for g in gt_results) / len(gt_results) if gt_results else 0
    report = {
        "tested": len(sample), "master_pid_ok": pid_ok, "master_pid_rate": round(pid_ok / max(1, len(times)), 3),
        "blocks_non200": blocks, "avg_page_sec": round(sum(times) / max(1, len(times)), 2),
        "gt_products": len(gt_results), "gt_avg_sibling_in_html_ratio": round(avg_ratio, 3),
        "gt_detail": gt_results,
    }
    os.makedirs("output", exist_ok=True)
    json.dump(report, open("output/pilot_mapping.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    print("\n" + "=" * 60 + "\nKONKLUSION")
    print(f"  master_pid udtrukket: {pid_ok}/{len(times)} ({report['master_pid_rate']:.0%})")
    print(f"  gennemsnit pr. side : {report['avg_page_sec']}s  |  blokeringer: {blocks}")
    print(f"  søster-SKUs i HTML  : {avg_ratio:.0%} (over {len(gt_results)} fler-variant-produkter)")
    if avg_ratio >= 0.9:
        print("  → ✅ SIDEN INDEHOLDER ALLE SØSTER-SKUs. Ét scrape pr. master giver hele gruppen")
        print("       → ~30k scrapes fanger BÅDE manglende OG fejl-merges. TIMER-projekt.")
    elif avg_ratio >= 0.3:
        print("  → ⚠️ DELVIST i HTML. Kræver verifikation — måske hybrid.")
    else:
        print("  → ❌ SØSTER-SKUs IKKE i HTML. Kræver per-SKU eller fetch_variant_skus (~150k). DAGE-projekt.")

if __name__ == "__main__":
    main()
