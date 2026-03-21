#!/usr/bin/env python3
"""Alpaca Historical REST API — behavior probe.

Calls the REAL Alpaca Data API v2 to document exact behavior for every
success and error scenario.  Output is designed to be saved as a CI
artifact so we can replicate the behaviour in broker-simulator.

Tested scenarios
    1. Authentication (valid / wrong key / wrong secret / no creds)
    2. Bars endpoint (valid / invalid symbol / date-range edge cases / pagination)
    3. Snapshots (valid / missing symbol)
    4. Latest endpoints (trade / quote / bar — valid & invalid symbol)
    5. Trades / Quotes range endpoints
    6. Feed parameter (iex / sip / invalid)
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime, timedelta
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")
BASE_URL = "https://data.alpaca.markets"
VALID_SYMBOL = "SPY"
INVALID_SYMBOL = "ZZZZZ999"
DELISTED_SYMBOL = "LEHM"  # long-gone
MULTI_SYMBOLS = "SPY,QQQ,MCD"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(name: str) -> str:
    val = os.getenv(name, "")
    if not val:
        print(f"WARNING: {name} not set", file=sys.stderr)
    return val


def _section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f" {title}")
    print("=" * 80)


def _case(label: str) -> None:
    print(f"\n--- {label} ---")


def _raw_request(
    path: str,
    params: dict[str, Any] | None = None,
    api_key: str | None = None,
    secret_key: str | None = None,
    timeout: int = 30,
) -> tuple[int, dict[str, str], str]:
    """Make a raw HTTP GET to Alpaca and return (status, headers, body)."""
    url = BASE_URL + path
    if params:
        url += "?" + urlencode({k: v for k, v in params.items() if v is not None})

    req = Request(url, method="GET")
    if api_key is not None:
        req.add_header("APCA-API-KEY-ID", api_key)
    if secret_key is not None:
        req.add_header("APCA-API-SECRET-KEY", secret_key)

    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode()
            headers = dict(resp.headers.items())
            return resp.status, headers, body
    except HTTPError as exc:
        body = exc.read().decode() if exc.fp else ""
        headers = dict(exc.headers.items()) if exc.headers else {}
        return exc.code, headers, body


def _print_response(status: int, headers: dict[str, str], body: str) -> None:
    print(f"  HTTP {status}")
    # Print selected headers
    for h in ("Content-Type", "X-Ratelimit-Limit", "X-Ratelimit-Remaining",
              "X-Ratelimit-Reset", "Retry-After"):
        if h.lower() in {k.lower() for k in headers}:
            # case-insensitive lookup
            for k, v in headers.items():
                if k.lower() == h.lower():
                    print(f"  {k}: {v}")
    # Pretty-print body (truncate if huge)
    try:
        parsed = json.loads(body)
        dumped = json.dumps(parsed, indent=2)
        if len(dumped) > 3000:
            print(f"  Body (truncated, total {len(dumped)} chars):")
            print("  " + dumped[:3000].replace("\n", "\n  "))
            print("  ... (truncated)")
        else:
            print(f"  Body:")
            print("  " + dumped.replace("\n", "\n  "))
    except (json.JSONDecodeError, ValueError):
        if len(body) > 2000:
            print(f"  Body (raw, truncated): {body[:2000]}...")
        else:
            print(f"  Body (raw): {body}")


def _run_case(label: str, path: str, params: dict[str, Any] | None = None, **kwargs: Any) -> None:
    _case(label)
    try:
        status, headers, body = _raw_request(path, params, **kwargs)
        _print_response(status, headers, body)
    except Exception:
        print(f"  EXCEPTION:")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Test sections
# ---------------------------------------------------------------------------

def test_auth(api_key: str, secret_key: str) -> None:
    _section("1) AUTHENTICATION BEHAVIOR")
    path = "/v2/stocks/bars"
    params = {"symbols": VALID_SYMBOL, "timeframe": "1Day", "limit": "1"}

    _run_case("1a) Valid credentials",
              path, params, api_key=api_key, secret_key=secret_key)

    _run_case("1b) Wrong API key (garbage)",
              path, params, api_key="pk_INVALID_GARBAGE_KEY_12345", secret_key=secret_key)

    _run_case("1c) Valid API key, wrong secret",
              path, params, api_key=api_key, secret_key="WRONG_SECRET_KEY_12345")

    _run_case("1d) No credentials at all",
              path, params, api_key=None, secret_key=None)

    _run_case("1e) Empty string credentials",
              path, params, api_key="", secret_key="")

    _run_case("1f) Only API key, no secret",
              path, params, api_key=api_key, secret_key=None)

    _run_case("1g) Only secret, no API key",
              path, params, api_key=None, secret_key=secret_key)


def test_bars(api_key: str, secret_key: str) -> None:
    _section("2) BARS ENDPOINT BEHAVIOR")
    now = datetime.now(tz=UTC)
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    future = (now + timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z")
    very_old = "2010-01-01T00:00:00Z"

    base_kw: dict[str, Any] = {"api_key": api_key, "secret_key": secret_key}

    _run_case("2a) Valid daily bars — single symbol",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "3"},
              **base_kw)

    _run_case("2b) Valid daily bars — multi symbol",
              "/v2/stocks/bars",
              {"symbols": MULTI_SYMBOLS, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "2"},
              **base_kw)

    _run_case("2c) Invalid/non-existent symbol",
              "/v2/stocks/bars",
              {"symbols": INVALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "3"},
              **base_kw)

    _run_case("2d) Mix of valid + invalid symbols",
              f"/v2/stocks/bars",
              {"symbols": f"{VALID_SYMBOL},{INVALID_SYMBOL}", "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "3"},
              **base_kw)

    _run_case("2e) Delisted symbol",
              "/v2/stocks/bars",
              {"symbols": DELISTED_SYMBOL, "timeframe": "1Day",
               "start": "2008-01-01T00:00:00Z", "end": "2008-09-01T00:00:00Z",
               "limit": "3"},
              **base_kw)

    _run_case("2f) Future dates only (start and end in the future)",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": future, "end": (now + timedelta(days=60)).strftime("%Y-%m-%dT00:00:00Z")},
              **base_kw)

    _run_case("2g) Start > End (invalid range)",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": yesterday, "end": week_ago},
              **base_kw)

    _run_case("2h) Very old date range (2010)",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": very_old, "end": "2010-01-15T00:00:00Z", "limit": "5"},
              **base_kw)

    _run_case("2i) limit=1 (check pagination token)",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "1"},
              **base_kw)

    _run_case("2j) limit=0",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "0"},
              **base_kw)

    _run_case("2k) No symbols parameter",
              "/v2/stocks/bars",
              {"timeframe": "1Day", "start": week_ago, "end": yesterday},
              **base_kw)

    _run_case("2l) Empty symbols parameter",
              "/v2/stocks/bars",
              {"symbols": "", "timeframe": "1Day",
               "start": week_ago, "end": yesterday},
              **base_kw)

    _run_case("2m) Minute timeframe",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Min",
               "start": (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "end": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "limit": "5"},
              **base_kw)

    _run_case("2n) Invalid timeframe value",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "GARBAGE",
               "start": week_ago, "end": yesterday},
              **base_kw)

    # Single-symbol endpoint
    _run_case("2o) Single-symbol path /v2/stocks/{symbol}/bars",
              f"/v2/stocks/{VALID_SYMBOL}/bars",
              {"timeframe": "1Day", "start": week_ago, "end": yesterday, "limit": "2"},
              **base_kw)

    _run_case("2p) Single-symbol path — invalid symbol",
              f"/v2/stocks/{INVALID_SYMBOL}/bars",
              {"timeframe": "1Day", "start": week_ago, "end": yesterday, "limit": "2"},
              **base_kw)

    # Sort order
    _run_case("2q) sort=desc",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "3", "sort": "desc"},
              **base_kw)

    _run_case("2r) sort=INVALID",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "sort": "INVALID"},
              **base_kw)


def test_snapshots(api_key: str, secret_key: str) -> None:
    _section("3) SNAPSHOTS ENDPOINT BEHAVIOR")
    base_kw: dict[str, Any] = {"api_key": api_key, "secret_key": secret_key}

    _run_case("3a) Valid snapshot — multi symbol",
              "/v2/stocks/snapshots",
              {"symbols": MULTI_SYMBOLS},
              **base_kw)

    _run_case("3b) Snapshot — invalid symbol",
              "/v2/stocks/snapshots",
              {"symbols": INVALID_SYMBOL},
              **base_kw)

    _run_case("3c) Snapshot — mix valid + invalid",
              "/v2/stocks/snapshots",
              {"symbols": f"{VALID_SYMBOL},{INVALID_SYMBOL}"},
              **base_kw)

    _run_case("3d) Snapshot — no symbols param",
              "/v2/stocks/snapshots",
              {},
              **base_kw)

    _run_case("3e) Snapshot — empty symbols",
              "/v2/stocks/snapshots",
              {"symbols": ""},
              **base_kw)

    _run_case("3f) Single-symbol snapshot path /v2/stocks/{symbol}/snapshot",
              f"/v2/stocks/{VALID_SYMBOL}/snapshot",
              {},
              **base_kw)

    _run_case("3g) Single-symbol snapshot — invalid symbol",
              f"/v2/stocks/{INVALID_SYMBOL}/snapshot",
              {},
              **base_kw)

    _run_case("3h) Snapshot with feed=iex",
              "/v2/stocks/snapshots",
              {"symbols": VALID_SYMBOL, "feed": "iex"},
              **base_kw)

    _run_case("3i) Snapshot with feed=sip",
              "/v2/stocks/snapshots",
              {"symbols": VALID_SYMBOL, "feed": "sip"},
              **base_kw)


def test_latest(api_key: str, secret_key: str) -> None:
    _section("4) LATEST ENDPOINTS BEHAVIOR")
    base_kw: dict[str, Any] = {"api_key": api_key, "secret_key": secret_key}

    # --- Latest trades ---
    _run_case("4a) Latest trades — valid symbols",
              "/v2/stocks/trades/latest",
              {"symbols": MULTI_SYMBOLS},
              **base_kw)

    _run_case("4b) Latest trades — invalid symbol",
              "/v2/stocks/trades/latest",
              {"symbols": INVALID_SYMBOL},
              **base_kw)

    _run_case("4c) Latest trade — single symbol path",
              f"/v2/stocks/{VALID_SYMBOL}/trades/latest",
              {},
              **base_kw)

    _run_case("4d) Latest trade — single symbol, invalid",
              f"/v2/stocks/{INVALID_SYMBOL}/trades/latest",
              {},
              **base_kw)

    # --- Latest quotes ---
    _run_case("4e) Latest quotes — valid symbols",
              "/v2/stocks/quotes/latest",
              {"symbols": MULTI_SYMBOLS},
              **base_kw)

    _run_case("4f) Latest quotes — invalid symbol",
              "/v2/stocks/quotes/latest",
              {"symbols": INVALID_SYMBOL},
              **base_kw)

    _run_case("4g) Latest quote — single symbol path",
              f"/v2/stocks/{VALID_SYMBOL}/quotes/latest",
              {},
              **base_kw)

    # --- Latest bars ---
    _run_case("4h) Latest bars — valid symbols",
              "/v2/stocks/bars/latest",
              {"symbols": MULTI_SYMBOLS},
              **base_kw)

    _run_case("4i) Latest bars — invalid symbol",
              "/v2/stocks/bars/latest",
              {"symbols": INVALID_SYMBOL},
              **base_kw)

    _run_case("4j) Latest bar — single symbol path",
              f"/v2/stocks/{VALID_SYMBOL}/bars/latest",
              {},
              **base_kw)


def test_trades_quotes_range(api_key: str, secret_key: str) -> None:
    _section("5) HISTORICAL TRADES / QUOTES RANGE")
    now = datetime.now(tz=UTC)
    base_kw: dict[str, Any] = {"api_key": api_key, "secret_key": secret_key}
    start_5m = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    _run_case("5a) Trades range — valid",
              "/v2/stocks/trades",
              {"symbols": VALID_SYMBOL, "start": start_5m, "end": end_now, "limit": "5"},
              **base_kw)

    _run_case("5b) Trades range — invalid symbol",
              "/v2/stocks/trades",
              {"symbols": INVALID_SYMBOL, "start": start_5m, "end": end_now, "limit": "5"},
              **base_kw)

    _run_case("5c) Single-symbol trades path",
              f"/v2/stocks/{VALID_SYMBOL}/trades",
              {"start": start_5m, "end": end_now, "limit": "5"},
              **base_kw)

    _run_case("5d) Quotes range — valid",
              "/v2/stocks/quotes",
              {"symbols": VALID_SYMBOL, "start": start_5m, "end": end_now, "limit": "5"},
              **base_kw)

    _run_case("5e) Quotes range — invalid symbol",
              "/v2/stocks/quotes",
              {"symbols": INVALID_SYMBOL, "start": start_5m, "end": end_now, "limit": "5"},
              **base_kw)


def test_feed_parameter(api_key: str, secret_key: str) -> None:
    _section("6) FEED PARAMETER BEHAVIOR")
    now = datetime.now(tz=UTC)
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    base_kw: dict[str, Any] = {"api_key": api_key, "secret_key": secret_key}

    _run_case("6a) feed=iex",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "2", "feed": "iex"},
              **base_kw)

    _run_case("6b) feed=sip",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "2", "feed": "sip"},
              **base_kw)

    _run_case("6c) feed=INVALID_FEED",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "2", "feed": "INVALID_FEED"},
              **base_kw)

    _run_case("6d) feed=otc",
              "/v2/stocks/bars",
              {"symbols": VALID_SYMBOL, "timeframe": "1Day",
               "start": week_ago, "end": yesterday, "limit": "2", "feed": "otc"},
              **base_kw)


def test_alpaca_py_sdk(api_key: str, secret_key: str) -> None:
    """Call the same endpoints via alpaca-py SDK to see how it wraps errors."""
    _section("7) ALPACA-PY SDK ERROR WRAPPING")

    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import (
            StockBarsRequest,
            StockSnapshotRequest,
        )
        from alpaca.data.timeframe import TimeFrame
    except ImportError:
        print("  alpaca-py not installed — skipping SDK tests")
        return

    now = datetime.now(tz=UTC)
    week_ago = now - timedelta(days=7)

    # 7a) Valid call via SDK
    _case("7a) SDK — valid bars request")
    try:
        client = StockHistoricalDataClient(api_key, secret_key)
        result = client.get_stock_bars(
            StockBarsRequest.model_validate({
                "symbol_or_symbols": [VALID_SYMBOL],
                "timeframe": TimeFrame.Day,
                "start": week_ago,
                "end": now,
                "limit": 2,
            })
        )
        print(f"  Success. Type: {type(result).__name__}")
        try:
            items = result[VALID_SYMBOL]
            print(f"  {VALID_SYMBOL}: {len(items)} bars")
            if items:
                bar = items[0]
                print(f"    First bar: {bar}")
                print(f"    Bar type: {type(bar).__name__}")
                print(f"    Bar attrs: {[a for a in dir(bar) if not a.startswith('_')]}")
        except KeyError as exc:
            print(f"  KeyError accessing result[{VALID_SYMBOL!r}]: {exc}")
    except Exception as exc:
        print(f"  Exception: {type(exc).__name__}: {exc}")

    # 7b) SDK with wrong credentials
    _case("7b) SDK — wrong API key")
    try:
        bad_client = StockHistoricalDataClient("pk_INVALID_KEY", secret_key)
        bad_client.get_stock_bars(
            StockBarsRequest.model_validate({
                "symbol_or_symbols": [VALID_SYMBOL],
                "timeframe": TimeFrame.Day,
                "start": week_ago,
                "end": now,
                "limit": 1,
            })
        )
        print("  Unexpectedly succeeded!")
    except Exception as exc:
        print(f"  Exception type: {type(exc).__name__}")
        print(f"  Exception message: {exc}")
        print(f"  Exception MRO: {[c.__name__ for c in type(exc).__mro__]}")
        if hasattr(exc, "status_code"):
            print(f"  status_code attr: {exc.status_code}")
        if hasattr(exc, "response"):
            print(f"  response attr: {exc.response}")

    # 7c) SDK with invalid symbol
    _case("7c) SDK — invalid symbol (bars)")
    try:
        client = StockHistoricalDataClient(api_key, secret_key)
        result = client.get_stock_bars(
            StockBarsRequest.model_validate({
                "symbol_or_symbols": [INVALID_SYMBOL],
                "timeframe": TimeFrame.Day,
                "start": week_ago,
                "end": now,
                "limit": 2,
            })
        )
        print(f"  Result type: {type(result).__name__}")
        try:
            items = result[INVALID_SYMBOL]
            print(f"  {INVALID_SYMBOL}: {len(items)} bars (empty list = no data)")
        except KeyError:
            print(f"  KeyError: {INVALID_SYMBOL} not in result keys")
            # Check what keys exist
            try:
                keys = list(result.data.keys()) if hasattr(result, "data") else "N/A"
                print(f"  Available keys: {keys}")
            except Exception:
                pass
    except Exception as exc:
        print(f"  Exception type: {type(exc).__name__}: {exc}")

    # 7d) SDK snapshot — invalid symbol
    _case("7d) SDK — snapshot of invalid symbol")
    try:
        client = StockHistoricalDataClient(api_key, secret_key)
        result = client.get_stock_snapshot(
            StockSnapshotRequest.model_validate({
                "symbol_or_symbols": [INVALID_SYMBOL],
            })
        )
        print(f"  Result type: {type(result).__name__}")
        print(f"  Result: {result}")
    except Exception as exc:
        print(f"  Exception type: {type(exc).__name__}: {exc}")

    # 7e) SDK — empty client (no creds)
    _case("7e) SDK — StockHistoricalDataClient with None creds")
    try:
        none_client = StockHistoricalDataClient(None, None)  # type: ignore[arg-type]
        result = none_client.get_stock_bars(
            StockBarsRequest.model_validate({
                "symbol_or_symbols": [VALID_SYMBOL],
                "timeframe": TimeFrame.Day,
                "start": week_ago,
                "end": now,
                "limit": 1,
            })
        )
        print(f"  Unexpectedly succeeded: {result}")
    except Exception as exc:
        print(f"  Exception type: {type(exc).__name__}: {exc}")

    # 7f) SDK — start > end
    _case("7f) SDK — start > end date")
    try:
        client = StockHistoricalDataClient(api_key, secret_key)
        result = client.get_stock_bars(
            StockBarsRequest.model_validate({
                "symbol_or_symbols": [VALID_SYMBOL],
                "timeframe": TimeFrame.Day,
                "start": now,
                "end": week_ago,
            })
        )
        print(f"  Result type: {type(result).__name__}")
        try:
            items = result[VALID_SYMBOL]
            print(f"  {VALID_SYMBOL}: {len(items)} bars")
        except KeyError:
            print(f"  KeyError: {VALID_SYMBOL} not in result")
    except Exception as exc:
        print(f"  Exception type: {type(exc).__name__}: {exc}")


def main() -> None:
    api_key = _env("ALPACA_API_KEY")
    secret_key = _env("ALPACA_SECRET_KEY")

    print("Alpaca Historical REST API — Behavior Probe")
    print(f"Timestamp: {datetime.now(tz=UTC).isoformat()}")
    print(f"Base URL: {BASE_URL}")
    print(f"Valid symbol: {VALID_SYMBOL}")
    print(f"Invalid symbol: {INVALID_SYMBOL}")

    if not api_key or not secret_key:
        print("\nERROR: ALPACA_API_KEY and ALPACA_SECRET_KEY must both be set.")
        sys.exit(1)

    test_auth(api_key, secret_key)
    test_bars(api_key, secret_key)
    test_snapshots(api_key, secret_key)
    test_latest(api_key, secret_key)
    test_trades_quotes_range(api_key, secret_key)
    test_feed_parameter(api_key, secret_key)
    test_alpaca_py_sdk(api_key, secret_key)

    _section("DONE")
    print("All scenarios executed. Review output above for exact Alpaca behavior.")


if __name__ == "__main__":
    main()
