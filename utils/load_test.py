#!/usr/bin/env python3
"""
Portfolio server load tester.

Tests two endpoints with increasing concurrency and prints a text chart of
response times and server RSS memory.

Endpoints tested:
  POST /portfolios/compare   (Allie Ideal vs Allie Actual, configurable)
  GET  /users/{id}/glance

Usage:
    python utils/load_test.py [OPTIONS]

    --base-url URL          Server base URL  (default: http://localhost:8080)
    --user-id   N           User ID          (default: 1)
    --portfolio-a NAME|ID   (default: "Allie Ideal")
    --portfolio-b NAME|ID   (default: "Allie Actual")
    --start-date YYYY-MM-DD (default: 2024-01-01)
    --end-date   YYYY-MM-DD (default: 2024-12-31)
    --concurrency 1,2,3,... Comma-separated levels (default: 1,2,3,5,7,10)
    --reps      N           Requests per level (default: 10)
    --server-name SUBSTR    Process name to monitor for RSS (default: portfolio)
"""

import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("Warning: psutil not installed — memory monitoring disabled.", file=sys.stderr)


# ─── HTTP helpers ──────────────────────────────────────────────────────────────

def http_get(url: str, headers: dict = None, timeout: float = 60.0) -> Tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def http_post(url: str, body: dict, headers: dict = None, timeout: float = 90.0) -> Tuple[int, bytes]:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


# ─── Portfolio discovery ───────────────────────────────────────────────────────

def get_all_portfolios(base_url: str, user_id: int) -> List[Dict]:
    """Return the full portfolio list for a user as a list of dicts."""
    url = f"{base_url}/users/{user_id}/portfolios"
    status, body = http_get(url, headers={"X-User-ID": str(user_id)})
    if status != 200:
        return []
    return json.loads(body)


def find_portfolio_id(base_url: str, user_id: int, name: str) -> Optional[int]:
    for p in get_all_portfolios(base_url, user_id):
        if p["name"].lower() == name.lower():
            return p["id"]
    return None


def resolve_portfolio(base_url: str, user_id: int, id_or_name: str) -> int:
    try:
        return int(id_or_name)
    except ValueError:
        pid = find_portfolio_id(base_url, user_id, id_or_name)
        if pid is None:
            sys.exit(f"Error: portfolio '{id_or_name}' not found for user {user_id}.")
        return pid



# ─── Memory monitor ────────────────────────────────────────────────────────────

class MemoryMonitor:
    """Polls a process's RSS in a background thread."""

    def __init__(self, pid: int, interval: float = 0.05):
        self._pid = pid
        self._interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._samples: List[int] = []

    def start(self) -> None:
        self._samples = []
        self._running = True
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()

    def stop(self) -> Tuple[int, int]:
        """Stop and return (min_rss_mib, max_rss_mib)."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        if not self._samples:
            return 0, 0
        mib = 1024 * 1024
        return min(self._samples) // mib, max(self._samples) // mib

    def _poll(self) -> None:
        try:
            proc = psutil.Process(self._pid)
            while self._running:
                try:
                    self._samples.append(proc.memory_info().rss)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break
                time.sleep(self._interval)
        except Exception:
            pass


def find_server_pid(name_substr: str) -> Optional[int]:
    """Find a process whose executable name (not full cmdline) contains name_substr.

    Matching only on name avoids false positives from processes like node whose
    *cmdline* happens to include a path like /home/user/react_portfolio/...
    """
    if not HAS_PSUTIL:
        return None
    my_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.pid == my_pid:
                continue
            name = proc.info['name'] or ""
            # Match on the executable basename only, not the full cmdline path.
            if name_substr in name:
                return proc.pid
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


# ─── Request helpers ───────────────────────────────────────────────────────────

def make_compare_fn(base_url: str, user_id: int, pid_a: int, pid_b: int,
                    start: str, end: str) -> Callable[[], Tuple[bool, float]]:
    url = f"{base_url}/portfolios/compare"
    body = {"portfolio_a": pid_a, "portfolio_b": pid_b,
            "start_period": start, "end_period": end}
    hdrs = {"X-User-ID": str(user_id)}

    def fn() -> Tuple[bool, float]:
        t0 = time.monotonic()
        status, _ = http_post(url, body, headers=hdrs)
        return status == 200, time.monotonic() - t0

    return fn


def make_random_compare_fn(base_url: str, user_id: int, portfolios: List[Dict],
                            start: str, end: str) -> Callable[[], Tuple[bool, float]]:
    """Returns a function that picks two distinct random portfolios on every call."""
    url = f"{base_url}/portfolios/compare"
    hdrs = {"X-User-ID": str(user_id)}

    def fn() -> Tuple[bool, float]:
        a, b = random.sample(portfolios, 2)
        body = {"portfolio_a": a["id"], "portfolio_b": b["id"],
                "start_period": start, "end_period": end}
        t0 = time.monotonic()
        status, _ = http_post(url, body, headers=hdrs)
        return status == 200, time.monotonic() - t0

    return fn


def make_glance_fn(base_url: str, user_id: int) -> Callable[[], Tuple[bool, float]]:
    url = f"{base_url}/users/{user_id}/glance"
    hdrs = {"X-User-ID": str(user_id)}

    def fn() -> Tuple[bool, float]:
        t0 = time.monotonic()
        status, _ = http_get(url, headers=hdrs)
        return status == 200, time.monotonic() - t0

    return fn


# ─── Load runner ───────────────────────────────────────────────────────────────

@dataclass
class LevelResult:
    concurrency: int
    times: List[float] = field(default_factory=list)
    errors: int = 0
    mem_min_mib: int = 0
    mem_max_mib: int = 0
    wall_time_s: float = 0.0

    @property
    def count(self) -> int:
        return len(self.times)

    @property
    def min_ms(self) -> float:
        return min(self.times) * 1000 if self.times else 0.0

    @property
    def avg_ms(self) -> float:
        return statistics.mean(self.times) * 1000 if self.times else 0.0

    @property
    def max_ms(self) -> float:
        return max(self.times) * 1000 if self.times else 0.0

    @property
    def p95_ms(self) -> float:
        if not self.times:
            return 0.0
        s = sorted(self.times)
        idx = min(int(0.95 * len(s)), len(s) - 1)
        return s[idx] * 1000

    @property
    def parallelism_ratio(self) -> float:
        """sum(individual times) / wall time — approaches concurrency for perfect parallelism."""
        if self.wall_time_s <= 0:
            return 0.0
        return sum(self.times) / self.wall_time_s


def run_level(concurrency: int, reps: int,
              request_fn: Callable[[], Tuple[bool, float]],
              monitor: Optional[MemoryMonitor]) -> LevelResult:
    result = LevelResult(concurrency=concurrency)
    if monitor:
        monitor.start()

    t_wall_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(request_fn) for _ in range(reps)]
        for f in as_completed(futures):
            try:
                ok, elapsed = f.result()
                if ok:
                    result.times.append(elapsed)
                else:
                    result.errors += 1
            except Exception as exc:
                result.errors += 1
                print(f"\n    [exception] {exc}", file=sys.stderr)
    result.wall_time_s = time.monotonic() - t_wall_start

    if monitor:
        result.mem_min_mib, result.mem_max_mib = monitor.stop()

    return result


# ─── Text chart renderer ───────────────────────────────────────────────────────

BAR_WIDTH = 36


def _bar(value: float, max_value: float) -> str:
    if max_value <= 0:
        return "░" * BAR_WIDTH
    filled = max(1, int(round(value / max_value * BAR_WIDTH))) if value > 0 else 0
    filled = min(filled, BAR_WIDTH)
    return "█" * filled + "░" * (BAR_WIDTH - filled)


def render_report(title: str, results: List[LevelResult], has_memory: bool) -> None:
    sep = "═" * 96

    print(f"\n{sep}")
    print(f"  {title}")
    print(sep)

    # Stats table
    hdr = (f"  {'C':>4}  {'Reps':>5}  {'Min':>8}  {'Avg':>8}  {'Max':>8}  {'p95':>8}"
           f"  {'Wall':>7}  {'req/s':>6}  {'Ratio':>7}  {'OK/N':>7}")
    if has_memory:
        hdr += f"  {'RSS min':>8}  {'RSS max':>8}"
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))

    for r in results:
        ok_of_n = f"{r.count}/{r.count + r.errors}"
        rps = (r.count + r.errors) / r.wall_time_s if r.wall_time_s > 0 else 0.0
        row = (f"  {r.concurrency:>4}  "
               f"{r.count + r.errors:>5}  "
               f"{r.min_ms:>7.0f}ms  "
               f"{r.avg_ms:>7.0f}ms  "
               f"{r.max_ms:>7.0f}ms  "
               f"{r.p95_ms:>7.0f}ms  "
               f"{r.wall_time_s:>6.1f}s  "
               f"{rps:>6.1f}  "
               f"{r.parallelism_ratio:>6.1f}x  "
               f"{ok_of_n:>7}")
        if has_memory:
            row += f"  {r.mem_min_mib:>6} MiB  {r.mem_max_mib:>6} MiB"
        print(row)

    # Avg response time bar chart
    max_avg = max((r.avg_ms for r in results), default=1) or 1
    print(f"\n  Avg response time  (scale: {max_avg:.0f} ms = full bar)\n")
    for r in results:
        bar = _bar(r.avg_ms, max_avg)
        print(f"  {r.concurrency:>4}c | {bar} | {r.avg_ms:.0f} ms")

    # Max response time bar chart
    max_max = max((r.max_ms for r in results), default=1) or 1
    print(f"\n  Max response time  (scale: {max_max:.0f} ms = full bar)\n")
    for r in results:
        bar = _bar(r.max_ms, max_max)
        print(f"  {r.concurrency:>4}c | {bar} | {r.max_ms:.0f} ms")

    # Parallelism ratio bar chart (ideal = concurrency)
    max_ratio = max((r.parallelism_ratio for r in results), default=1) or 1
    print(f"\n  Parallelism ratio  (ideal: ratio ≈ concurrency;  scale: {max_ratio:.1f}x = full bar)\n")
    for r in results:
        ideal = r.concurrency
        bar = _bar(r.parallelism_ratio, max_ratio)
        print(f"  {r.concurrency:>4}c | {bar} | {r.parallelism_ratio:.1f}x  (ideal {ideal}x)")

    if has_memory:
        max_rss = max((r.mem_max_mib for r in results), default=1) or 1
        print(f"\n  RSS max  (scale: {max_rss} MiB = full bar)\n")
        for r in results:
            bar = _bar(r.mem_max_mib, max_rss)
            print(f"  {r.concurrency:>4}c | {bar} | {r.mem_max_mib} MiB")

    print()


# ─── Main ─────────────────────────────────────────────────────────────────────

def _run_section(label: str, levels: List[int], cycles: int,
                 request_fn: Callable[[], Tuple[bool, float]],
                 server_pid: Optional[int], has_memory: bool) -> List[LevelResult]:
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"  {cycles} cycles × concurrency reps per level  ({len(levels)} levels)")
    print(f"{'─'*60}")
    results: List[LevelResult] = []
    for c in levels:
        reps = c * cycles
        mon = MemoryMonitor(server_pid) if has_memory else None
        print(f"  c={c:>3}  reps={reps:>4} ...", end="", flush=True)
        r = run_level(c, reps, request_fn, mon)
        results.append(r)
        mem = f"  RSS {r.mem_min_mib}–{r.mem_max_mib} MiB" if has_memory else ""
        print(f"  avg={r.avg_ms:6.0f}ms  wall={r.wall_time_s:5.1f}s"
              f"  ratio={r.parallelism_ratio:5.1f}x"
              f"  ok={r.count}/{r.count+r.errors}{mem}")
    return results


def main() -> None:
    p = argparse.ArgumentParser(description="Portfolio server load tester")
    p.add_argument("--base-url",            default="http://localhost:8080")
    p.add_argument("--user-id",             type=int, default=1)
    p.add_argument("--portfolio-a",         default="Allie Ideal")
    p.add_argument("--portfolio-b",         default="Allie Actual")
    p.add_argument("--start-date",          default="2024-01-01")
    p.add_argument("--end-date",            default="2024-12-31")
    p.add_argument("--compare-concurrency", default="1,10,20,30,40,50,60,70,80,90,100",
                   help="Concurrency levels for /compare tests")
    p.add_argument("--glance-concurrency",  default="1,2,3,5,7,10",
                   help="Concurrency levels for /glance test")
    p.add_argument("--cycles",              type=int, default=2,
                   help="Saturation cycles per level; reps = concurrency × cycles")
    p.add_argument("--server-name",         default="portfolio")
    args = p.parse_args()

    base_url = args.base_url.rstrip("/")
    user_id = args.user_id
    compare_levels = [int(x) for x in args.compare_concurrency.split(",")]
    glance_levels  = [int(x) for x in args.glance_concurrency.split(",")]
    cycles = args.cycles

    # ── Health check ──
    print(f"Checking {base_url}/health ...")
    try:
        status, body = http_get(f"{base_url}/health", timeout=5)
        if status != 200:
            sys.exit(f"Health check failed: HTTP {status}")
        print(f"  OK — {body.decode().strip()}")
    except Exception as e:
        sys.exit(f"Cannot reach server: {e}")

    # ── Fetch portfolio list (used for both fixed resolution and random sampling) ──
    print(f"\nFetching portfolio list for user {user_id} ...")
    all_portfolios = get_all_portfolios(base_url, user_id)
    if len(all_portfolios) < 2:
        sys.exit(f"Error: user {user_id} has fewer than 2 portfolios.")
    print(f"  Found {len(all_portfolios)} portfolios: "
          f"{', '.join(p['name'] for p in all_portfolios)}")

    # ── Resolve fixed portfolio IDs ──
    print(f"\nResolving fixed portfolios ...")
    pa = resolve_portfolio(base_url, user_id, args.portfolio_a)
    pb = resolve_portfolio(base_url, user_id, args.portfolio_b)
    print(f"  A: '{args.portfolio_a}' → id {pa}")
    print(f"  B: '{args.portfolio_b}' → id {pb}")
    print(f"  Date range: {args.start_date} → {args.end_date}")
    print(f"\nRandom compare: two portfolios re-sampled from the pool on every request.")

    # ── Locate server process for memory ──
    server_pid = find_server_pid(args.server_name)
    has_memory = HAS_PSUTIL and server_pid is not None
    if has_memory:
        print(f"\nMonitoring RSS of PID {server_pid} ({args.server_name})")
    else:
        print("\nMemory monitoring: disabled" +
              ("" if HAS_PSUTIL else " (install psutil to enable)"))

    fixed_compare_fn = make_compare_fn(base_url, user_id, pa, pb,
                                       args.start_date, args.end_date)
    rand_compare_fn  = make_random_compare_fn(base_url, user_id, all_portfolios,
                                              args.start_date, args.end_date)
    glance_fn = make_glance_fn(base_url, user_id)

    # ── Run sections ──
    fixed_results = _run_section(
        "POST /portfolios/compare — FIXED ALLIE PORTFOLIOS",
        compare_levels, cycles, fixed_compare_fn, server_pid, has_memory,
    )
    rand_results = _run_section(
        f"POST /portfolios/compare — RANDOM PORTFOLIOS  (re-sampled every request, pool={len(all_portfolios)})",
        compare_levels, cycles, rand_compare_fn, server_pid, has_memory,
    )
    glance_results = _run_section(
        f"GET /users/{user_id}/glance",
        glance_levels, cycles, glance_fn, server_pid, has_memory,
    )

    # ── Final reports ──
    render_report("/compare — FIXED ALLIE PORTFOLIOS", fixed_results, has_memory)
    render_report(f"/compare — RANDOM PORTFOLIOS  (pool={len(all_portfolios)}, re-sampled per request)",
                  rand_results, has_memory)
    render_report(f"GET /users/{user_id}/glance", glance_results, has_memory)

    print("═" * 82)
    print("  Load test complete.")
    print("═" * 82)


if __name__ == "__main__":
    main()
