"""Alpaca REST API mock — serves test data in Alpaca wire format.

Implements the subset of Alpaca Data API v2 that the ingestion SDK uses.
Responses use Alpaca's short-key JSON schema so the real ``alpaca-py``
SDK can parse them transparently via ``url_override``.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Query

from simulator.alpaca.data_store import AlpacaDataStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Wire-format converters  (test-data long keys → Alpaca REST short keys)
# ---------------------------------------------------------------------------

def _bar_wire(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "t": raw["timestamp"],
        "o": raw["open"],
        "h": raw["high"],
        "l": raw["low"],
        "c": raw["close"],
        "v": int(raw["volume"]) if raw.get("volume") is not None else 0,
        "n": int(raw["trade_count"]) if raw.get("trade_count") is not None else 0,
        "vw": raw.get("vwap", 0.0),
    }


def _trade_wire(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "t": raw["timestamp"],
        "p": raw["price"],
        "s": int(raw["size"]),
        "x": raw.get("exchange", ""),
        "i": raw.get("id", 0),
        "c": raw.get("conditions", []),
        "z": raw.get("tape", ""),
    }


def _quote_wire(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "t": raw["timestamp"],
        "bp": raw["bid_price"],
        "ap": raw["ask_price"],
        "bs": int(raw["bid_size"]),
        "as": int(raw["ask_size"]),
        "bx": raw.get("bid_exchange", ""),
        "ax": raw.get("ask_exchange", ""),
        "c": raw.get("conditions", []),
        "z": raw.get("tape", ""),
    }


def _snapshot_wire(raw: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"minuteBar": None}
    if raw.get("latest_trade"):
        result["latestTrade"] = _trade_wire(raw["latest_trade"])
    if raw.get("latest_quote"):
        result["latestQuote"] = _quote_wire(raw["latest_quote"])
    if raw.get("daily_bar"):
        result["dailyBar"] = _bar_wire(raw["daily_bar"])
    if raw.get("prev_daily_bar"):
        result["prevDailyBar"] = _bar_wire(raw["prev_daily_bar"])
    return result


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

def create_router(store: AlpacaDataStore) -> APIRouter:
    """Create FastAPI router with Alpaca-compatible REST endpoints."""
    router = APIRouter(prefix="/v2/stocks")

    # ── Multi-symbol endpoints ────────────────────────────────────────────

    @router.get("/bars")
    async def get_bars(
        symbols: str = Query(...),
        timeframe: str = Query("1Day"),
        start: str | None = Query(None),
        end: str | None = Query(None),
        limit: int | None = Query(None),
        feed: str | None = Query(None),
        sort: str = Query("asc"),
    ) -> dict[str, Any]:
        symbol_list = [s.strip() for s in symbols.split(",")]
        bars: dict[str, list[dict[str, Any]]] = {}
        for sym in symbol_list:
            bars[sym] = [_bar_wire(b) for b in store.bars.get(sym, [])]
        return {"bars": bars, "next_page_token": None}

    @router.get("/snapshots")
    async def get_snapshots(
        symbols: str = Query(...),
        feed: str | None = Query(None),
    ) -> dict[str, Any]:
        symbol_list = [s.strip() for s in symbols.split(",")]
        result: dict[str, Any] = {}
        for sym in symbol_list:
            snap = store.snapshots.get(sym)
            if snap:
                result[sym] = _snapshot_wire(snap)
        return result

    @router.get("/trades/latest")
    async def get_latest_trades(
        symbols: str = Query(...),
        feed: str | None = Query(None),
    ) -> dict[str, Any]:
        symbol_list = [s.strip() for s in symbols.split(",")]
        trades: dict[str, Any] = {}
        for sym in symbol_list:
            sym_trades = store.trades.get(sym, [])
            if sym_trades:
                trades[sym] = _trade_wire(sym_trades[-1])
        return {"trades": trades}

    # ── Single-symbol endpoints ───────────────────────────────────────────

    @router.get("/{symbol}/bars")
    async def get_symbol_bars(
        symbol: str,
        timeframe: str = Query("1Day"),
        start: str | None = Query(None),
        end: str | None = Query(None),
        limit: int | None = Query(None),
        feed: str | None = Query(None),
        sort: str = Query("asc"),
    ) -> dict[str, Any]:
        raw_bars = store.bars.get(symbol, [])
        return {
            "bars": {symbol: [_bar_wire(b) for b in raw_bars]},
            "next_page_token": None,
        }

    @router.get("/{symbol}/snapshot")
    async def get_symbol_snapshot(
        symbol: str,
        feed: str | None = Query(None),
    ) -> dict[str, Any]:
        snap = store.snapshots.get(symbol)
        return _snapshot_wire(snap) if snap else {}

    @router.get("/{symbol}/trades/latest")
    async def get_symbol_latest_trade(
        symbol: str,
        feed: str | None = Query(None),
    ) -> dict[str, Any]:
        sym_trades = store.trades.get(symbol, [])
        if sym_trades:
            return {"trade": _trade_wire(sym_trades[-1])}
        return {"trade": None}

    return router
