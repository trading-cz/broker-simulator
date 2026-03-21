#!/usr/bin/env python3
"""Fetch historical bars and snapshots from Alpaca and write to JSON-lines files.

Outputs (under data/alpaca/):
  - bars_daily.jsonl   — daily bars for configured symbols
  - snapshots.jsonl    — synthetic morning snapshots for configured symbols

All snapshots are synthesised from daily bar data (daily_bar, prev_daily_bar and
synthetic latest_trade/latest_quote built from the open price at 9:30 AM ET) so
any past trading day can be replayed as if the broker-simulator were running live that
morning.  A ``date`` field is included so replay code can select records by date.

Single-day mode (default / --days 1):
  Appends bar records and synthetic snapshots for yesterday.

Multi-day mode (--days N where N > 1):
  Truncates both files first, then writes bars and snapshots for every trading
  day in the requested range.  Avoids duplicate lines.

Usage:
  python fetch_alpaca_test_data.py           # fetch yesterday (default)
  python fetch_alpaca_test_data.py --days 80 # backfill last 80 days (truncates files)

Requires env vars: ALPACA_API_KEY, ALPACA_SECRET_KEY
Optional env var: SYMBOLS  (comma-separated, e.g. "SPY,QQQ,TSLA")
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")

_DEFAULT_SYMBOLS = ["SPY", "QQQ", "MCD", "KO", "IBM", "TSLA"]
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "alpaca"

# US market open: 9:30 AM Eastern = 14:30 UTC
_OPEN_HOUR_UTC = 14
_OPEN_MINUTE_UTC = 30


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required env var {name}")
    return value


def _synthetic_snapshot_from_bar(
    symbol: str,
    bar: object,
    prev_bar: object | None,
) -> dict[str, object]:
    """Build a synthetic morning-snapshot dict from a daily bar for historical replay.

    ``latest_trade`` and ``latest_quote`` are synthesised from the *open* price
    at 9:30 AM ET so the record matches what the simulator would see at market open.
    A ``date`` field is included so replay code can filter records by trading date.
    """
    bar_ts: datetime = getattr(bar, "timestamp")
    open_price = float(getattr(bar, "open"))
    open_ts = bar_ts.replace(
        hour=_OPEN_HOUR_UTC, minute=_OPEN_MINUTE_UTC, second=0, microsecond=0,
        tzinfo=timezone.utc,
    ).isoformat()

    result: dict[str, object] = {
        "symbol": symbol,
        "date": bar_ts.strftime("%Y-%m-%d"),
        "latest_trade": {
            "symbol": symbol,
            "timestamp": open_ts,
            "price": open_price,
            "size": 100.0,
        },
        "latest_quote": {
            "symbol": symbol,
            "timestamp": open_ts,
            "bid_price": round(open_price - 0.01, 2),
            "ask_price": round(open_price + 0.01, 2),
            "bid_size": 100.0,
            "ask_size": 100.0,
        },
        "daily_bar": bar.model_dump(mode="json"),  # type: ignore[union-attr]
    }
    if prev_bar is not None:
        result["prev_daily_bar"] = prev_bar.model_dump(mode="json")  # type: ignore[union-attr]
    return result


def _build_synthetic_snapshots(
    bars_by_symbol: dict[str, list],
    symbols: list[str],
) -> list[dict[str, object]]:
    """Return synthetic morning-snapshot records for every trading day in bars_by_symbol.

    Output is sorted by (date, symbol) for deterministic results.
    """
    snapshots: list[dict[str, object]] = []

    for sym in symbols:
        # Index bars by date; keep last bar per date if API returns duplicates.
        bars_by_date: dict[str, object] = {}
        for bar in bars_by_symbol.get(sym, []):
            bars_by_date[getattr(bar, "timestamp").strftime("%Y-%m-%d")] = bar

        sorted_dates = sorted(bars_by_date)
        for i, date_str in enumerate(sorted_dates):
            bar = bars_by_date[date_str]
            prev_bar = bars_by_date.get(sorted_dates[i - 1]) if i > 0 else None
            snapshots.append(_synthetic_snapshot_from_bar(sym, bar, prev_bar))

    snapshots.sort(key=lambda s: (s["date"], s["symbol"]))  # type: ignore[index]
    return snapshots


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch Alpaca historical bars and snapshots and write to data files."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        metavar="N",
        help=(
            "Number of calendar days to fetch counting back from yesterday (default: 1)."
            " When N > 1 both output files are truncated before writing to avoid duplicates,"
            " and snapshots are synthesised from daily bar data for every trading day."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Fetch data from Alpaca and write to JSON-lines files."""
    args = _parse_args()
    days: int = max(1, args.days)
    backfill = days > 1

    api_key = _require_env("ALPACA_API_KEY")
    secret_key = _require_env("ALPACA_SECRET_KEY")

    symbols_env = os.getenv("SYMBOLS", "")
    SYMBOLS = [s.strip().upper() for s in symbols_env.split(",") if s.strip()] or _DEFAULT_SYMBOLS
    print(f"Symbols: {SYMBOLS}")

    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    client = StockHistoricalDataClient(api_key, secret_key)
    now_utc = datetime.now(tz=UTC)
    end_utc = now_utc - timedelta(days=1)        # yesterday (fully closed bars)
    start_utc = end_utc - timedelta(days=days - 1)

    # Truncate on backfill to avoid duplicate lines; append on daily single-day run.
    write_mode = "w" if backfill else "a"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    bars_path = OUTPUT_DIR / "bars_daily.jsonl"
    snaps_path = OUTPUT_DIR / "snapshots.jsonl"

    verb = "Wrote" if backfill else "Appended"

    # ── 1) Daily bars ────────────────────────────────────────
    # Fetch 7 extra calendar days before start so we have a prev_daily_bar
    # candidate for the earliest requested trading day.
    bar_fetch_start = start_utc - timedelta(days=7)

    print(
        f"Fetching daily bars for {SYMBOLS} "
        f"({start_utc.date()} → {end_utc.date()}, {days} day(s))"
        + (" [backfill — files will be truncated]" if backfill else " [append]")
    )
    bar_request = StockBarsRequest.model_validate({
        "symbol_or_symbols": SYMBOLS,
        "timeframe": TimeFrame.Day,
        "start": bar_fetch_start,
        "end": end_utc,
    })
    bar_result = client.get_stock_bars(bar_request)

    bars_by_symbol: dict[str, list] = {}
    for sym in SYMBOLS:
        try:
            bars_by_symbol[sym] = list(bar_result[sym])
        except KeyError:
            print(f"  WARNING: no bars returned for {sym}")
            bars_by_symbol[sym] = []

    count = 0
    with bars_path.open(write_mode, encoding="utf-8") as fh:
        for sym in SYMBOLS:
            for bar in bars_by_symbol[sym]:
                fh.write(json.dumps(bar.model_dump(mode="json")) + "\n")
                count += 1
    print(f"  {verb} {count} bar records → {bars_path}")

    # ── 2) Snapshots ─────────────────────────────────────────
    # Always synthesise snapshots from bar data so every day can be replayed as
    # a live morning run without hitting the live API.
    print(f"Building synthetic morning snapshots for {SYMBOLS} ({days} day(s)) …")
    snaps = _build_synthetic_snapshots(bars_by_symbol, SYMBOLS)
    snap_count = 0
    with snaps_path.open(write_mode, encoding="utf-8") as fh:
        for snap in snaps:
            fh.write(json.dumps(snap) + "\n")
            snap_count += 1
    print(f"  {verb} {snap_count} snapshot records → {snaps_path}")

    print(f"\nDone. Files updated in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
