"""
Benchmark: Bottleneck Analysis of Centralized 2PL
===================================================
"""
import json, os, sys, time, threading, random
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from glm.lock_manager import GlobalLockManager, LockType

BRANCHES      = ["BR001-HCM", "BR002-HN", "BR003-DN", "BR004-CT", "BR005-HP"]
LOG_DIR       = os.path.join(os.path.dirname(__file__), "logs")
HOT_ACCOUNTS  = [f"ACC{str(i).zfill(4)}" for i in range(1, 11)]
COLD_ACCOUNTS = [f"ACC{str(i).zfill(4)}" for i in range(11, 1001)]
LOCK_HOLD_MS  = 0.015
TX_PER_RUN    = 100
os.makedirs(LOG_DIR, exist_ok=True)


def pick_accounts(hot_ratio=0.8):
    if random.random() < hot_ratio:
        return random.choice(HOT_ACCOUNTS), random.choice(HOT_ACCOUNTS)
    return random.choice(COLD_ACCOUNTS), random.choice(COLD_ACCOUNTS)


def run_one_transaction(glm, branch):
    src, dst = pick_accounts()
    items = sorted({src, dst})
    tx_id = glm.begin_transaction(branch)
    granted = all(
        glm.request_lock(tx_id, item, LockType.WRITE, timeout_s=10.0)
        for item in items
    )
    if granted:
        time.sleep(LOCK_HOLD_MS + random.uniform(0, LOCK_HOLD_MS))
        glm.release_all_locks(tx_id, commit=True)
    else:
        glm.release_all_locks(tx_id, commit=False)
    tx = glm.tx_table.get(tx_id)
    return {"success": granted, "wait_ms": tx.total_wait_ms if tx else 0.0}


def run_level(n_concurrent):
    glm = GlobalLockManager()
    results = []
    r_lock = threading.Lock()
    counter = {"done": 0}
    c_lock = threading.Lock()

    def worker():
        branch = random.choice(BRANCHES)
        while True:
            with c_lock:
                if counter["done"] >= TX_PER_RUN:
                    return
                counter["done"] += 1
            r = run_one_transaction(glm, branch)
            with r_lock:
                results.append(r)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(n_concurrent)]
    t0 = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join(timeout=120)
    elapsed = time.perf_counter() - t0

    ok       = sum(1 for r in results if r["success"])
    avg_wait = sum(r["wait_ms"] for r in results) / max(1, len(results))
    tps      = len(results) / max(0.001, elapsed)

    # Lấy lock log thật từ GLM
    raw_log = glm.get_lock_queue_log()[:100]
    lock_log = []
    for r in raw_log:
        lock_log.append({
            "tx_id":     r["tx_id"],
            "item_id":   r["item_id"],
            "lock_type": str(r["lock_type"]).split(".")[-1],
            "branch_id": r["branch_id"],
            "wait_ms":   round(r["wait_time_ms"], 1),
            "granted":   r["granted"],
        })

    return {
        "n_concurrent":    n_concurrent,
        "n_transactions":  len(results),
        "elapsed_s":       round(elapsed, 3),
        "throughput_tps":  round(tps, 2),
        "success_rate":    round(ok / max(1, len(results)) * 100, 2),
        "avg_wait_ms":     round(avg_wait, 3),
        "avg_duration_ms": round(elapsed / max(1, len(results)) * 1000, 3),
        "aborts":          len(results) - ok,
        "lock_log":        lock_log,
    }


def run_benchmark():
    print("\n" + "="*72)
    print("  C2PL BOTTLENECK BENCHMARK  -  HIGH CONTENTION MODE")
    print("  Hot accounts: %d accts | Hold: %dms | Hot ratio: 80%%" % (len(HOT_ACCOUNTS), LOCK_HOLD_MS*1000))
    print("="*72)
    print("%12s %8s %14s %10s %8s" % ("Concurrency","TPS","Avg Wait(ms)","Success%","Aborts"))
    print("-"*72)

    levels = [10, 20, 30, 50, 75, 100]
    all_results = []

    for n in levels:
        r = run_level(n)
        all_results.append(r)
        print("%12d %8.1f %14.1f %9.1f%% %8d" % (
            n, r["throughput_tps"], r["avg_wait_ms"], r["success_rate"], r["aborts"]))

    print("="*72)

    # Lưu JSON (không có lock_log để file gọn)
    out = []
    for r in all_results:
        d = dict(r)
        del d["lock_log"]
        out.append(d)
    with open(os.path.join(LOG_DIR, "benchmark_results.json"), "w") as f:
        json.dump(out, f, indent=2)
    print("\nDetailed results -> logs/benchmark_results.json")

    # Bottleneck analysis
    print("\n  BOTTLENECK ANALYSIS:")
    base = all_results[0]["avg_wait_ms"]
    for r in all_results:
        factor = r["avg_wait_ms"] / max(0.001, base)
        bar = "#" * int(factor * 5)
        print("  %3d concurrent -> wait %7.1fms  %s (%.1fx)" % (
            r["n_concurrent"], r["avg_wait_ms"], bar, factor))

    return all_results


def update_dashboard(all_results):
    dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard", "index.html")
    if not os.path.exists(dashboard_path):
        return

    with open(dashboard_path, "r", encoding="utf-8") as f:
        html = f.read()

    # --- Cập nhật biểu đồ số liệu ---
    concurrency = [r["n_concurrent"]   for r in all_results]
    wait_ms     = [r["avg_wait_ms"]    for r in all_results]
    tps_data    = [r["throughput_tps"] for r in all_results]
    success_pct = [r["success_rate"]   for r in all_results]
    abort_pct   = [round(100 - r["success_rate"], 2) for r in all_results]
    total_tx    = sum(r["n_transactions"] for r in all_results)
    max_tps     = max(tps_data)
    peak_wait   = max(wait_ms)
    max_abort   = max(abort_pct)
    best_tps_run = all_results[tps_data.index(max_tps)]["n_concurrent"]
    peak_wait_run = all_results[wait_ms.index(peak_wait)]["n_concurrent"]
    abort_run = all_results[abort_pct.index(max_abort)]["n_concurrent"]

    new_js = (
        "// Benchmark data - AUTO UPDATED by benchmark.py\n"
        "const concurrency = " + str(concurrency) + ";\n"
        "const waitMs      = " + str(wait_ms) + ";\n"
        "const tps         = " + str([int(t) for t in tps_data]) + ";\n"
        "const successPct  = " + str(success_pct) + ";\n"
        "const abortPct    = " + str(abort_pct) + ";"
    )

    start_idx = -1
    for marker in ["// Benchmark data from our runs", "// Benchmark data - AUTO UPDATED by benchmark.py"]:
        start_idx = html.find(marker)
        if start_idx != -1:
            break

    end_marker = "const GRID ="
    end_idx = html.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        return

    html = html[:start_idx] + new_js + "\n\n" + end_marker + html[end_idx + len(end_marker):]

    # --- Inject lock log thật vào bảng ---
    last_log = all_results[-1].get("lock_log", [])
    if last_log:
        max_w = max((r["wait_ms"] for r in last_log), default=1) or 1
        rows_html = "\n"
        for r in last_log:
            lt      = r["lock_type"]
            bar_w   = min(100, int(r["wait_ms"] / max_w * 100))
            granted = r["granted"]
            badge   = "badge-ok" if granted else "badge-fail"
            status  = "GRANTED" if granted else "TIMEOUT"
            color   = "var(--accent)" if lt == "READ" else "var(--accent4)"
            lt_lc   = lt.lower()
            rows_html += (
                "<tr>"
                "<td style='color:var(--accent)'>" + r["tx_id"] + "</td>"
                "<td>" + r["item_id"] + "</td>"
                "<td><span class='badge badge-" + lt_lc + "'>" + lt + "</span></td>"
                "<td>" + r["branch_id"] + "</td>"
                "<td>" + str(r["wait_ms"]) + "ms"
                "<span class='wait-bar-bg'>"
                "<span class='wait-bar' style='width:" + str(bar_w) + "%;background:" + color + "'>"
                "</span></span></td>"
                "<td><span class='badge " + badge + "'>" + status + "</span></td>"
                "</tr>\n"
            )

        s = html.find('<tbody id="logBody">')
        e = html.find("</tbody>", s)
        if s != -1 and e != -1:
            html = html[:s + len('<tbody id="logBody">')] + rows_html + html[e:]
    
    import re

    html = re.sub(
        r'(<div class="kpi-label">MAX THROUGHPUT</div>\s*<div class="kpi-value">).*?(</div>)',
        rf'\g<1>{max_tps:.1f}\g<2>',
        html,
        flags=re.S
    )

    html = re.sub(
        r'(TPS @ )\d+( concurrent)',
        rf'TPS @ {best_tps_run}\2',
        html
    )

    html = re.sub(
        r'(<div class="kpi-label">AVG GLM WAIT</div>\s*<div class="kpi-value">).*?(</div>)',
        rf'\g<1>{peak_wait:.1f}\g<2>',
        html,
        flags=re.S
    )

    html = re.sub(
        r'(ms peak \()\d+( conc\.\))',
        rf'ms peak ({peak_wait_run} conc.)',
        html
    )

    html = re.sub(
        r'(<div class="kpi-label">ABORT RATE</div>\s*<div class="kpi-value">).*?(</div>)',
        rf'\g<1>{max_abort:.1f}%\g<2>',
        html,
        flags=re.S
    )

    html = re.sub(
        r'(Max at )\d+( concurrent)',
        rf'Max at {abort_run} concurrent',
        html
    )

    html = re.sub(
        r'(<div class="kpi-label">TRANSACTIONS RUN</div>\s*<div class="kpi-value">).*?(</div>)',
        rf'\g<1>{total_tx}\g<2>',
        html,
        flags=re.S
    )
    
    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    results = run_benchmark()
    update_dashboard(results)
