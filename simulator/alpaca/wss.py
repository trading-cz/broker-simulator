"""Alpaca WebSocket streaming mock — replays test data via Alpaca WSS protocol.

Protocol (matches real Alpaca stream at ``wss://stream.data.alpaca.markets/v2/{feed}``)::

    1. Connect           → server sends  [{"T":"success","msg":"connected"}]
    2. Auth              → server sends  [{"T":"success","msg":"authenticated"}]
    3. Subscribe         → server sends  [{"T":"subscription",...}]
    4. Stream            → server pushes [{"T":"t",...}] / [{"T":"b",...}] in a loop
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from simulator.alpaca.data_store import AlpacaDataStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Wire-format converters  (test-data long keys → Alpaca WSS message format)
# ---------------------------------------------------------------------------

def _bar_ws(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "T": "b",
        "S": raw["symbol"],
        "t": raw["timestamp"],
        "o": raw["open"],
        "h": raw["high"],
        "l": raw["low"],
        "c": raw["close"],
        "v": int(raw["volume"]) if raw.get("volume") is not None else 0,
        "n": int(raw["trade_count"]) if raw.get("trade_count") is not None else 0,
        "vw": raw.get("vwap", 0.0),
    }


def _trade_ws(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "T": "t",
        "S": raw["symbol"],
        "t": raw["timestamp"],
        "p": raw["price"],
        "s": int(raw["size"]),
        "x": raw.get("exchange", ""),
        "i": raw.get("id", 0),
        "c": raw.get("conditions", []),
        "z": raw.get("tape", ""),
    }


# ---------------------------------------------------------------------------
# Stream loop
# ---------------------------------------------------------------------------

async def _stream_loop(
    ws: WebSocket,
    store: AlpacaDataStore,
    trade_symbols: set[str],
    bar_symbols: set[str],
    replay_interval: float,
) -> None:
    """Replay test data as Alpaca WSS messages.  Loops until cancelled."""
    while True:
        for symbol in sorted(trade_symbols):
            for trade in store.trades.get(symbol, []):
                await ws.send_json([_trade_ws(trade)])
                await asyncio.sleep(replay_interval)

        for symbol in sorted(bar_symbols):
            for bar in store.bars.get(symbol, []):
                await ws.send_json([_bar_ws(bar)])
                await asyncio.sleep(replay_interval)

        # Pause before replaying the full dataset again
        await asyncio.sleep(replay_interval * 5)


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

def create_ws_router(store: AlpacaDataStore, replay_interval: float) -> APIRouter:
    """Create FastAPI router with Alpaca-compatible WebSocket streaming."""
    router = APIRouter()

    @router.websocket("/v2/{feed}")
    async def ws_endpoint(websocket: WebSocket, feed: str) -> None:
        await websocket.accept()
        logger.info("WSS client connected (feed=%s)", feed)

        # ── 1. Connected ─────────────────────────────────────────────────
        await websocket.send_json([{"T": "success", "msg": "connected"}])

        # ── 2. Auth ──────────────────────────────────────────────────────
        try:
            auth_msg = await websocket.receive_json()
        except WebSocketDisconnect:
            return

        if auth_msg.get("action") != "auth":
            await websocket.send_json(
                [{"T": "error", "msg": "auth required", "code": 401}]
            )
            await websocket.close()
            return

        await websocket.send_json([{"T": "success", "msg": "authenticated"}])
        logger.info("WSS client authenticated")

        # ── 3. Subscribe ─────────────────────────────────────────────────
        try:
            sub_msg = await websocket.receive_json()
        except WebSocketDisconnect:
            return

        if sub_msg.get("action") != "subscribe":
            await websocket.send_json(
                [{"T": "error", "msg": "must x` first"}]
            )
            await websocket.close()
            return

        trade_symbols: set[str] = set(sub_msg.get("trades", []))
        bar_symbols: set[str] = set(sub_msg.get("bars", []))

        # Expand wildcard
        if "*" in trade_symbols:
            trade_symbols = set(store.trades.keys())
        if "*" in bar_symbols:
            bar_symbols = set(store.bars.keys())

        await websocket.send_json([{
            "T": "subscription",
            "trades": sorted(trade_symbols),
            "quotes": [],
            "bars": sorted(bar_symbols),
            "updatedBars": [],
            "dailyBars": [],
            "statuses": [],
            "lulds": [],
            "corrections": sorted(trade_symbols),
            "cancelErrors": sorted(trade_symbols),
        }])
        logger.info(
            "WSS subscribed: trades=%s bars=%s",
            sorted(trade_symbols), sorted(bar_symbols),
        )

        # ── 4. Stream data ───────────────────────────────────────────────
        stream_task = asyncio.create_task(
            _stream_loop(websocket, store, trade_symbols, bar_symbols, replay_interval)
        )
        try:
            # Keep receiving to detect disconnect (or future subscribe changes)
            while True:
                await websocket.receive_json()
        except WebSocketDisconnect:
            logger.info("WSS client disconnected")
        finally:
            stream_task.cancel()
            try:
                await stream_task
            except (asyncio.CancelledError, Exception):
                pass

    return router
