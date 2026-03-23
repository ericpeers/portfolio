#!/usr/bin/env python3
"""
check_price_gaps.py - Detect gaps in fact_price data vs fact_price_range claims.

Measures gaps in TRADING DAYS (weekends and NYSE holidays excluded) to eliminate
normal market-closure noise. Three gap types are reported per security:

  head     - trading days in [range_start, first_price - 1]: range claims data
             before the first actual price row exists
  tail     - trading days in [last_price + 1, range_end]: range claims data
             after the last actual price row
  internal - trading days strictly between two consecutive price rows

Filtering defaults:
  --min-gap 2       Flag any security with 2+ missing trading days in a gap
  --volume-gap 1    Flag high-volume securities with 1+ missing trading day
  --volume-threshold 500000  "High volume" = avg daily volume >= this

Usage:
    python3 utils/check_price_gaps.py
    python3 utils/check_price_gaps.py --min-gap 5
    python3 utils/check_price_gaps.py --ticker AAPL
    python3 utils/check_price_gaps.py --min-gap 3 --volume-threshold 1000000
"""

import argparse
import os
import sys
from datetime import date, timedelta
from urllib.parse import urlparse, unquote


# ── NYSE holiday calendar (ported from internal/services/trading_calendar.go) ────────

AD_HOC_CLOSURES = frozenset([
    date(2001, 9, 11), date(2001, 9, 12), date(2001, 9, 13), date(2001, 9, 14),
    date(2004, 6, 11),
    date(2007, 1, 2),
    date(2012, 10, 29), date(2012, 10, 30),
    date(2018, 12, 5),
    date(2025, 1, 9),
])

EASTER_SUNDAYS = {
    1990: date(1990, 4, 15), 1991: date(1991, 3, 31), 1992: date(1992, 4, 19),
    1993: date(1993, 4, 11), 1994: date(1994, 4,  3), 1995: date(1995, 4, 16),
    1996: date(1996, 4,  7), 1997: date(1997, 3, 30), 1998: date(1998, 4, 12),
    1999: date(1999, 4,  4), 2000: date(2000, 4, 23), 2001: date(2001, 4, 15),
    2002: date(2002, 3, 31), 2003: date(2003, 4, 20), 2004: date(2004, 4, 11),
    2005: date(2005, 3, 27), 2006: date(2006, 4, 16), 2007: date(2007, 4,  8),
    2008: date(2008, 3, 23), 2009: date(2009, 4, 12), 2010: date(2010, 4,  4),
    2011: date(2011, 4, 24), 2012: date(2012, 4,  8), 2013: date(2013, 3, 31),
    2014: date(2014, 4, 20), 2015: date(2015, 4,  5), 2016: date(2016, 3, 27),
    2017: date(2017, 4, 16), 2018: date(2018, 4,  1), 2019: date(2019, 4, 21),
    2020: date(2020, 4, 12), 2021: date(2021, 4,  4), 2022: date(2022, 4, 17),
    2023: date(2023, 4,  9), 2024: date(2024, 3, 31), 2025: date(2025, 4, 20),
    2026: date(2026, 4,  5), 2027: date(2027, 3, 28), 2028: date(2028, 4, 16),
    2029: date(2029, 4,  1), 2030: date(2030, 4, 21),
}


def _meeus_easter(year):
    a = year % 19; b = year // 100; c = year % 100
    d = b // 4;    e = b % 4;       f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19*a + b - d - g + 15) % 30
    i = c // 4;    k = c % 4
    l = (32 + 2*e + 2*i - h - k) % 7
    m = (a + 11*h + 22*l) // 451
    month = (h + l - 7*m + 114) // 31
    day   = ((h + l - 7*m + 114) % 31) + 1
    return date(year, month, day)


def _good_friday(year):
    easter = EASTER_SUNDAYS.get(year) or _meeus_easter(year)
    return easter - timedelta(days=2)


def _observed(year, month, day):
    h = date(year, month, day)
    if h.weekday() == 5: return h - timedelta(days=1)  # Sat → Fri
    if h.weekday() == 6: return h + timedelta(days=1)  # Sun → Mon
    return h


def _nth_weekday(year, month, weekday, n):
    """n-th occurrence of weekday (0=Mon … 6=Sun) in month/year."""
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    return d + timedelta(weeks=n - 1)


def _last_weekday(year, month, weekday):
    """Last occurrence of weekday in month/year."""
    last = date(year, month + 1, 1) - timedelta(days=1) if month < 12 \
           else date(year + 1, 1, 1) - timedelta(days=1)
    while last.weekday() != weekday:
        last -= timedelta(days=1)
    return last


_holiday_cache: dict = {}


def _build_holidays(year):
    h = {
        _observed(year,     1,  1),             # New Year's Day
        _observed(year + 1, 1,  1),             # Dec 31 when next Jan 1 is Sat
        _nth_weekday(year, 1, 0, 3),            # MLK Day (3rd Mon Jan)
        _nth_weekday(year, 2, 0, 3),            # Presidents' Day (3rd Mon Feb)
        _good_friday(year),                     # Good Friday
        _last_weekday(year, 5, 0),              # Memorial Day (last Mon May)
        _observed(year,     7,  4),             # Independence Day
        _nth_weekday(year, 9, 0, 1),            # Labor Day (1st Mon Sep)
        _nth_weekday(year, 11, 3, 4),           # Thanksgiving (4th Thu Nov)
        _observed(year,    12, 25),             # Christmas
    }
    if year >= 2022:
        h.add(_observed(year, 6, 19))           # Juneteenth
    return h


def is_trading_day(d: date) -> bool:
    if d in AD_HOC_CLOSURES:
        return False
    if d.weekday() >= 5:                        # Saturday or Sunday
        return False
    if d.year not in _holiday_cache:
        _holiday_cache[d.year] = _build_holidays(d.year)
    return d not in _holiday_cache[d.year]


def build_prefix_sum(start: date, end: date) -> dict:
    """
    Returns dict: date → cumulative trading-day count from start through d (inclusive).
    Includes a sentinel at start-1 day → 0 so closed-interval lookups always have a left bound.
    """
    prefix = {start - timedelta(days=1): 0}
    count = 0
    d = start
    while d <= end:
        if is_trading_day(d):
            count += 1
        prefix[d] = count
        d += timedelta(days=1)
    return prefix


def count_trading_days_closed(prefix: dict, a: date, b: date) -> int:
    """Trading days in closed interval [a, b]. Returns 0 if b < a."""
    if b < a:
        return 0
    return prefix.get(b, 0) - prefix.get(a - timedelta(days=1), 0)


# ── DB helpers ────────────────────────────────────────────────────────────────────────

def load_env(path=".env"):
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
    except FileNotFoundError:
        pass


def parse_pg_url(url):
    p = urlparse(url)
    kw = dict(host=p.hostname, port=p.port or 5432,
              dbname=p.path.lstrip("/"), user=p.username,
              password=unquote(p.password) if p.password else None)
    return {k: v for k, v in kw.items() if v is not None}


# ── SQL ───────────────────────────────────────────────────────────────────────────────
#
# SQL pre-filters to reduce rows before Python does the precise trading-day count:
#
# Internal gaps:
#   - gap_days >= 4           (always potentially real: Fri→Tue, Thu→Mon, etc.)
#   - gap_days = 3 AND        (3-day gap not starting on Friday: Mon→Thu or Tue→Fri
#     DOW(gap_start) != 5      which always have 2 missing trading days)
#   - gap_days = 2 AND        (1 missing trading day, only for high-volume)
#     avg_volume >= threshold
#   Excluded: gap_days = 3 AND DOW = 5 (Fri→Mon normal weekend, always 0 missing)
#
# Head/tail gaps: all gaps with gap_days >= 1 (only 49K rows total, cheap).

QUERY = """
WITH avg_volumes AS (
    SELECT security_id,
           AVG(volume)::bigint    AS avg_volume,
           AVG(volume * close)    AS avg_dollar_vol
    FROM fact_price
    GROUP BY security_id
),
first_last AS (
    SELECT security_id, MIN(date) AS first_date, MAX(date) AS last_date
    FROM fact_price
    GROUP BY security_id
),
raw_internals AS (
    SELECT
        fp.security_id,
        ds.ticker,
        LAG(fp.date) OVER (PARTITION BY fp.security_id ORDER BY fp.date) AS gap_start,
        fp.date                                                            AS gap_end,
        fp.date - LAG(fp.date) OVER (PARTITION BY fp.security_id ORDER BY fp.date) AS gap_days
    FROM fact_price fp
    JOIN dim_security ds ON ds.id = fp.security_id
    WHERE (%s::text IS NULL OR ds.ticker = %s)
),
filtered_internals AS (
    SELECT ri.security_id, ri.ticker, ri.gap_start, ri.gap_end, ri.gap_days,
           'internal' AS gap_type
    FROM raw_internals ri
    JOIN avg_volumes av ON av.security_id = ri.security_id
    WHERE ri.gap_start IS NOT NULL
      AND (
            -- 4+ calendar days: always potentially suspicious
            ri.gap_days >= 4
            -- 3 days NOT starting Friday: Mon→Thu or Tue→Fri (2 missing trading days)
         OR (ri.gap_days = 3 AND EXTRACT(DOW FROM ri.gap_start) <> 5)
            -- 2 calendar days on high-volume: 1 missing trading day
         OR (ri.gap_days = 2 AND av.avg_volume >= %s)
      )
),
head_tail AS (
    SELECT
        fpr.security_id,
        ds.ticker,
        fpr.start_date          AS gap_start,
        fl.first_date           AS gap_end,
        fl.first_date - fpr.start_date AS gap_days,
        'head' AS gap_type
    FROM fact_price_range fpr
    JOIN dim_security ds ON ds.id = fpr.security_id
    JOIN first_last fl    ON fl.security_id = fpr.security_id
    WHERE (%s::text IS NULL OR ds.ticker = %s)
      AND fl.first_date - fpr.start_date >= 1
    UNION ALL
    SELECT
        fpr.security_id,
        ds.ticker,
        fl.last_date            AS gap_start,
        fpr.end_date            AS gap_end,
        fpr.end_date - fl.last_date AS gap_days,
        'tail' AS gap_type
    FROM fact_price_range fpr
    JOIN dim_security ds ON ds.id = fpr.security_id
    JOIN first_last fl    ON fl.security_id = fpr.security_id
    WHERE (%s::text IS NULL OR ds.ticker = %s)
      AND fpr.end_date - fl.last_date >= 1
),
all_gaps AS (
    SELECT security_id, ticker, gap_start, gap_end, gap_days, gap_type FROM filtered_internals
    UNION ALL
    SELECT security_id, ticker, gap_start, gap_end, gap_days, gap_type FROM head_tail
)
SELECT g.security_id, g.ticker, g.gap_type,
       g.gap_start, g.gap_end, g.gap_days,
       COALESCE(av.avg_volume,     0) AS avg_volume,
       COALESCE(av.avg_dollar_vol, 0) AS avg_dollar_vol
FROM all_gaps g
LEFT JOIN avg_volumes av ON av.security_id = g.security_id
ORDER BY g.ticker ASC, g.gap_start ASC;
"""


def fmt_vol(v):
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000:     return f"{v/1_000:.0f}K"
    return str(v)


def main():
    ap = argparse.ArgumentParser(description="Detect price data gaps (in trading days)")
    ap.add_argument("--min-gap", type=int, default=2,
                    help="Missing trading days to flag for any security (default: 2)")
    ap.add_argument("--volume-gap", type=int, default=1,
                    help="Missing trading days to flag for high-volume securities (default: 1)")
    ap.add_argument("--volume-threshold", type=int, default=500_000,
                    help="Avg daily volume threshold for 'high volume' (default: 500000)")
    ap.add_argument("--low-volume-cap", type=int, default=25_000,
                    help="Avg daily volume below which gaps < --low-volume-min are suppressed (default: 25000)")
    ap.add_argument("--low-dollar-cap", type=int, default=25_000,
                    help="Avg daily dollar volume below which gaps < --low-volume-min are suppressed (default: 25000)")
    ap.add_argument("--low-volume-min", type=int, default=30,
                    help="Min missing trading days to show for low-volume/low-dollar securities (default: 30)")
    ap.add_argument("--floor-dvol", type=int, default=10_000,
                    help="Avg daily dollar volume below which ALL gaps are suppressed regardless of size "
                         "(default: 10000). Catches effectively-dead stocks where even a 6-month gap "
                         "is not actionable.")
    ap.add_argument("--floor-vol", type=int, default=500,
                    help="Avg daily share volume below which ALL gaps are suppressed regardless of size "
                         "(default: 500). Catches high-priced illiquid stocks that dollar-volume "
                         "alone misses — e.g. 192 shares/day at $64 = $12K/day passes the dollar "
                         "floor but is not a real market.")
    ap.add_argument("--ticker", default=None,
                    help="Restrict analysis to one ticker")
    ap.add_argument("--skip-head", action="store_true",
                    help="Skip head gaps entirely (range-start before first price is usually "
                         "an IPO/data-source boundary, not a real data gap)")
    ap.add_argument("--skip-suffix", default="F,Q",
                    help="Comma-separated OTC ticker suffixes to skip entirely (default: F,Q). "
                         "F=foreign OTC security (non-US trading calendar), Q=bankrupt/in-default. "
                         "Add Y for ADRs. These have fundamentally different data availability.")
    ap.add_argument("--chronic-gap-count", type=int, default=20,
                    help="A ticker with this many or more gaps where no single gap exceeds "
                         "--chronic-max-gap days is a chronically incomplete data source. "
                         "Collapsed to one summary line instead of N individual lines (default: 20).")
    ap.add_argument("--provider-gap-min", type=int, default=50,
                    help="Report any single trading date missing from this many or more tickers "
                         "as a likely provider outage (default: 50). Only scans gaps with "
                         "<= --provider-gap-scan-depth missing days to avoid iterating over "
                         "long individual-security outages.")
    ap.add_argument("--provider-gap-scan-depth", type=int, default=10,
                    help="Max missing-trading-day count a gap may have to be included in "
                         "provider-gap date counting (default: 10). Gaps larger than this are "
                         "individual security issues, not provider outages.")
    ap.add_argument("--chronic-max-gap", type=int, default=15,
                    help="Max missing trading days a gap may have for the ticker to be considered "
                         "chronic (default: 15). Gaps above this indicate real outages, not noise.")
    args = ap.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root  = os.path.dirname(script_dir)
    load_env(os.path.join(repo_root, ".env"))

    pg_url = os.environ.get("PG_URL")
    if not pg_url:
        sys.exit("ERROR: PG_URL not set. Add it to .env or export it.")

    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        sys.exit("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")

    conn = psycopg2.connect(**parse_pg_url(pg_url))
    conn.autocommit = True

    print(f"Scanning for gaps  (min_gap={args.min_gap} trading days, "
          f"volume_gap={args.volume_gap} @ vol>={fmt_vol(args.volume_threshold)}, "
          f"low-vol suppress: vol<{fmt_vol(args.low_volume_cap)} requires {args.low_volume_min}+ missing days"
          + (f", ticker={args.ticker}" if args.ticker else "") + ") ...")

    # 1. Fetch the date range we need to build the trading-day prefix sum over.
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(start_date), MAX(end_date) FROM fact_price_range;")
        row = cur.fetchone()
    if not row or not row[0]:
        print("No rows in fact_price_range.")
        return

    range_start, range_end = row[0], row[1]
    print(f"Building NYSE trading calendar {range_start} → {range_end} ...", end=" ", flush=True)
    prefix = build_prefix_sum(range_start, range_end)
    print("done.")

    # 2. Fetch candidate gaps from DB.
    params = (
        args.ticker, args.ticker,       # filtered_internals WHERE ticker
        args.volume_threshold,          # volume_gap = 2 filter
        args.ticker, args.ticker,       # head WHERE ticker
        args.ticker, args.ticker,       # tail WHERE ticker
    )
    print("Querying gaps ...", end=" ", flush=True)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(QUERY, params)
        rows = cur.fetchall()
    print(f"{len(rows):,} candidate gaps fetched.")
    conn.close()

    skip_suffixes = tuple(s.strip().upper() for s in args.skip_suffix.split(",") if s.strip())

    # 3. For each candidate, compute precise missing-trading-days and classify.
    shown      = []   # printed in full
    suppressed = []   # low-volume small gaps: counted in stats but not printed
    skipped_suffix = []  # F/Q/Y tickers — different calendar, not actionable
    one_day = timedelta(days=1)

    for row in rows:
        gap_type  = row["gap_type"]
        gap_start = row["gap_start"]
        gap_end   = row["gap_end"]
        avg_vol   = row["avg_volume"]

        if gap_type == "internal":
            missing = count_trading_days_closed(prefix, gap_start + one_day, gap_end - one_day)
        elif gap_type == "head":
            missing = count_trading_days_closed(prefix, gap_start, gap_end - one_day)
        else:  # tail
            missing = count_trading_days_closed(prefix, gap_start + one_day, gap_end)

        if missing <= 0:
            continue  # normal weekend / holiday cluster

        dollar_vol  = row["avg_dollar_vol"]

        if args.skip_head and gap_type == "head":
            continue

        ticker = row["ticker"]
        if skip_suffixes and ticker.upper().endswith(skip_suffixes):
            skipped_suffix.append(ticker)
            continue

        entry = {
            "sec_id":    row["security_id"],
            "ticker":    row["ticker"],
            "gap_type":  gap_type,
            "gap_start": gap_start,
            "gap_end":   gap_end,
            "missing":   missing,
            "avg_vol":   avg_vol,
            "dollar_vol": dollar_vol,
        }

        # Determine the display threshold for this security.
        # Dollar-volume check comes first: a penny stock with millions of shares/day
        # but $13K/day in dollar volume should be treated as low-activity.
        # Use AVG(volume * close) — not AVG(volume)*AVG(close) — because high-price
        # periods correlate with high volume, making the product-of-averages an overestimate.
        if dollar_vol < args.floor_dvol or avg_vol < args.floor_vol:
            # Effectively-dead stock: suppress everything, still count in stats.
            # Two independent signals: low dollar volume OR low share count.
            # Share-count catches high-priced illiquid stocks (e.g. 192 shares/day
            # at $64 = $12K/day passes the dollar floor but is not a real market).
            suppressed.append(entry)
            continue
        elif avg_vol < args.low_volume_cap or dollar_vol < args.low_dollar_cap:
            # Low share-volume OR low dollar-volume: suppress small gaps; still count.
            if missing < args.low_volume_min:
                suppressed.append(entry)
                continue
            threshold = args.low_volume_min
        elif avg_vol >= args.volume_threshold:
            threshold = args.volume_gap
        else:
            threshold = args.min_gap

        if missing < threshold:
            continue

        shown.append(entry)

    if not shown and not suppressed and not skipped_suffix:
        print("\nNo gaps found matching the criteria.")
        return

    # 3b. Provider-gap analysis: for small gaps, enumerate each missing trading day
    # and accumulate a date → set-of-tickers counter.  Large gaps (individual security
    # outages) are excluded via --provider-gap-scan-depth so they don't inflate counts.
    date_tickers: dict = {}
    for r in shown:
        if r["missing"] > args.provider_gap_scan_depth:
            continue
        d = r["gap_start"] + one_day
        while d < r["gap_end"]:
            if is_trading_day(d):
                if d not in date_tickers:
                    date_tickers[d] = set()
                date_tickers[d].add(r["ticker"])
            d += one_day

    # 4. Group shown entries by ticker, apply chronic-source collapse, then print.
    from collections import defaultdict
    by_ticker = defaultdict(list)
    for r in shown:
        by_ticker[r["ticker"]].append(r)

    print()
    shown_ticker_count = 0
    chronic_ticker_count = 0
    shown_type_counts   = {"head": 0, "tail": 0, "internal": 0}
    chronic_type_counts = {"head": 0, "tail": 0, "internal": 0}

    for ticker, entries in by_ticker.items():
        gap_count = len(entries)
        max_missing = max(r["missing"] for r in entries)
        is_chronic = (gap_count >= args.chronic_gap_count
                      and max_missing < args.chronic_max_gap)

        if is_chronic:
            chronic_ticker_count += 1
            for r in entries:
                chronic_type_counts[r["gap_type"]] = chronic_type_counts.get(r["gap_type"], 0) + 1
            r0 = entries[0]
            print(f"── {ticker}  (sec_id={r0['sec_id']}, avg_vol={fmt_vol(r0['avg_vol'])})  "
                  f"[CHRONIC: {gap_count} gaps, max {max_missing}d — data source unreliable]")
        else:
            shown_ticker_count += 1
            r0 = entries[0]
            print(f"── {ticker}  (sec_id={r0['sec_id']}, avg_vol={fmt_vol(r0['avg_vol'])})")
            for r in entries:
                gap_type = r["gap_type"]
                missing  = r["missing"]
                shown_type_counts[gap_type] = shown_type_counts.get(gap_type, 0) + 1
                cal_days = (r["gap_end"] - r["gap_start"]).days
                severity = " *** LARGE ***" if missing >= 10 else (" **" if missing >= 5 else "")
                print(f"   [{gap_type:8s}] {r['gap_start']} → {r['gap_end']}"
                      f"  ({missing} missing trading days, {cal_days} cal days){severity}")

    # 5. Summary stats.
    sup_type_counts = {"head": 0, "tail": 0, "internal": 0}
    sup_tickers = set()
    for r in suppressed:
        sup_type_counts[r["gap_type"]] = sup_type_counts.get(r["gap_type"], 0) + 1
        sup_tickers.add(r["ticker"])

    shown_gaps = sum(shown_type_counts.values())
    print()
    print(f"── Shown gaps ─────────────────────────────────────────────────")
    print(f"Tickers:  {shown_ticker_count}  ({shown_gaps} gaps)")
    print(f"  head:     {shown_type_counts.get('head', 0)}")
    print(f"  tail:     {shown_type_counts.get('tail', 0)}")
    print(f"  internal: {shown_type_counts.get('internal', 0)}")
    if chronic_ticker_count:
        chronic_gaps = sum(chronic_type_counts.values())
        print()
        print(f"── Chronic (≥{args.chronic_gap_count} gaps, max<{args.chronic_max_gap}d) — one line each ──")
        print(f"Tickers:  {chronic_ticker_count}  ({chronic_gaps} gaps collapsed)")
    if suppressed:
        print()
        print(f"── Suppressed (dvol<${args.floor_dvol:,} or vol<{args.floor_vol} always; "
              f"dvol<${args.low_dollar_cap:,} or vol<{fmt_vol(args.low_volume_cap)} if gap<{args.low_volume_min}d) ──")
        print(f"Tickers:  {len(sup_tickers)}  ({len(suppressed)} gaps)")
    if skipped_suffix:
        print()
        skipped_set = set(skipped_suffix)
        print(f"── Skipped suffix ({args.skip_suffix}) ───────────────────────────")
        print(f"Tickers:  {len(skipped_set)}  ({len(skipped_suffix)} gap rows)")

    # Provider-gap report: dates missing from many tickers simultaneously.
    provider_gaps = sorted(
        ((d, tickers) for d, tickers in date_tickers.items()
         if len(tickers) >= args.provider_gap_min),
        key=lambda x: -len(x[1]),
    )
    if provider_gaps:
        print()
        print(f"── Likely provider gaps (≥{args.provider_gap_min} tickers missing same day) ──")
        for d, tickers in provider_gaps:
            ticker_list = ", ".join(sorted(tickers))
            print(f"  {d}  {len(tickers):4d} tickers missing: {ticker_list}")


if __name__ == "__main__":
    main()
