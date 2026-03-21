#!/usr/bin/env python3
"""Alpaca WebSocket Streaming — behavior probe.

Connects to the REAL Alpaca streaming endpoint using raw websockets
(not alpaca-py SDK) to document exact protocol messages for every
scenario.  Output is saved as a CI artifact so we can replicate
the behaviour in broker-simulator.

Tested scenarios
    1. Connection to valid / invalid feeds
    2. Authentication: valid / wrong key / wrong secret / no auth /
       empty creds / auth with wrong action
    3. Subscription: valid symbols / invalid symbols / wildcard / empty
    4. Data reception: capture first N messages after subscribe
    5. Protocol edge cases: double auth, subscribe before auth, garbage
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import traceback
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")

# Alpaca streaming endpoints (v2)
WSS_BASE = "wss://stream.data.alpaca.markets/v2"
VALID_SYMBOL = "SPY"
INVALID_SYMBOL = "ZZZZZ999"
DATA_WAIT_SEC = 10  # seconds to wait for streaming data after subscribe
MSG_TIMEOUT = 5     # timeout for individual message receive


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


def _pp(msg: Any) -> str:
    """Pretty-print a message for logging."""
    if isinstance(msg, (dict, list)):
        return json.dumps(msg, indent=2)
    return str(msg)


async def _connect_and_run(
    feed: str,
    steps: list[dict[str, Any]],
    label: str,
    timeout: float = 15.0,
) -> None:
    """Generic WSS test runner.

    *steps* is a list of actions::

        {"action": "recv"}              — receive and print one message
        {"action": "recv_many", "count": 5, "timeout": 10}
        {"action": "send", "data": {...}}
        {"action": "sleep", "seconds": 1}
    """
    _case(label)

    try:
        import websockets  # noqa: PLC0415
    except ImportError:
        print("  websockets package not installed — skipping")
        return

    url = f"{WSS_BASE}/{feed}"
    print(f"  URL: {url}")

    try:
        async with asyncio.timeout(timeout):
            async with websockets.connect(url) as ws:
                for step in steps:
                    action = step["action"]

                    if action == "recv":
                        try:
                            msg = await asyncio.wait_for(ws.recv(), MSG_TIMEOUT)
                            parsed = json.loads(msg) if isinstance(msg, str) else msg
                            print(f"  << RECV: {_pp(parsed)}")
                        except TimeoutError:
                            print("  << RECV: TIMEOUT (no message)")
                        except Exception as exc:
                            print(f"  << RECV ERROR: {type(exc).__name__}: {exc}")

                    elif action == "send":
                        data = step["data"]
                        raw = json.dumps(data)
                        print(f"  >> SEND: {_pp(data)}")
                        await ws.send(raw)

                    elif action == "recv_many":
                        count = step.get("count", 5)
                        wait = step.get("timeout", DATA_WAIT_SEC)
                        print(f"  (waiting up to {wait}s for {count} messages)")
                        received = 0
                        deadline = asyncio.get_event_loop().time() + wait
                        while received < count:
                            remaining = deadline - asyncio.get_event_loop().time()
                            if remaining <= 0:
                                break
                            try:
                                msg = await asyncio.wait_for(ws.recv(), min(remaining, MSG_TIMEOUT))
                                parsed = json.loads(msg) if isinstance(msg, str) else msg
                                print(f"  << MSG[{received}]: {_pp(parsed)}")
                                received += 1
                            except TimeoutError:
                                print(f"  << No more messages after {received} received")
                                break
                            except Exception as exc:
                                print(f"  << ERROR: {type(exc).__name__}: {exc}")
                                break
                        if received == 0:
                            print(f"  << No messages received in {wait}s")

                    elif action == "sleep":
                        secs = step["seconds"]
                        print(f"  (sleeping {secs}s)")
                        await asyncio.sleep(secs)

    except TimeoutError:
        print(f"  TIMEOUT: overall test exceeded {timeout}s")
    except Exception as exc:
        print(f"  CONNECTION ERROR: {type(exc).__name__}: {exc}")
        traceback.print_exc()


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

async def test_connection() -> None:
    _section("1) CONNECTION BEHAVIOR")

    # 1a) Connect to iex feed — expect connected message
    await _connect_and_run("iex", [
        {"action": "recv"},  # should get [{"T":"success","msg":"connected"}]
    ], "1a) Connect to iex feed")

    # 1b) Connect to sip feed
    await _connect_and_run("sip", [
        {"action": "recv"},
    ], "1b) Connect to sip feed")

    # 1c) Connect to invalid feed
    await _connect_and_run("invalidfeed", [
        {"action": "recv"},
    ], "1c) Connect to invalid feed name")

    # 1d) Connect to empty feed
    await _connect_and_run("", [
        {"action": "recv"},
    ], "1d) Connect to empty feed (URL: .../v2/)")


async def test_auth(api_key: str, secret_key: str) -> None:
    _section("2) AUTHENTICATION BEHAVIOR")

    # Full valid auth flow
    await _connect_and_run("iex", [
        {"action": "recv"},  # connected
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},  # auth response
    ], "2a) Valid auth — correct key + secret")

    # Wrong API key
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": "pk_WRONG_KEY_12345", "secret": secret_key}},
        {"action": "recv"},
    ], "2b) Wrong API key")

    # Valid key, wrong secret
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": "WRONG_SECRET_12345"}},
        {"action": "recv"},
    ], "2c) Valid key, wrong secret")

    # Empty credentials
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": "", "secret": ""}},
        {"action": "recv"},
    ], "2d) Empty string credentials")

    # No auth — send subscribe directly
    await _connect_and_run("iex", [
        {"action": "recv"},  # connected
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},  # what happens?
    ], "2e) Skip auth — send subscribe first")

    # Wrong action instead of auth
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "listen", "key": api_key, "secret": secret_key}},
        {"action": "recv"},
    ], "2f) Wrong action field (listen instead of auth)")

    # Auth with missing key field
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "secret": secret_key}},
        {"action": "recv"},
    ], "2g) Auth with missing key field")

    # Auth with missing secret field
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": api_key}},
        {"action": "recv"},
    ], "2h) Auth with missing secret field")

    # Send garbage JSON
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": "this is not json"},
        {"action": "recv"},
    ], "2i) Send plain string instead of JSON")

    # Send numeric data
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": 42},
        {"action": "recv"},
    ], "2j) Send number instead of JSON object")


async def test_subscription(api_key: str, secret_key: str) -> None:
    _section("3) SUBSCRIPTION BEHAVIOR")

    auth_steps = [
        {"action": "recv"},  # connected
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},  # authenticated
    ]

    # 3a) Subscribe to valid symbol — trades
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},  # subscription confirmation
    ], "3a) Subscribe trades — valid symbol")

    # 3b) Subscribe to invalid symbol
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [INVALID_SYMBOL]}},
        {"action": "recv"},
    ], "3b) Subscribe trades — invalid symbol")

    # 3c) Subscribe to mix of valid + invalid
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL, INVALID_SYMBOL]}},
        {"action": "recv"},
    ], "3c) Subscribe trades — mix valid + invalid symbols")

    # 3d) Subscribe with wildcard
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": ["*"]}},
        {"action": "recv"},
    ], "3d) Subscribe trades — wildcard (*)")

    # 3e) Empty subscription
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [], "quotes": [], "bars": []}},
        {"action": "recv"},
    ], "3e) Subscribe — all empty arrays")

    # 3f) Subscribe to bars
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "bars": [VALID_SYMBOL]}},
        {"action": "recv"},
    ], "3f) Subscribe bars — valid symbol")

    # 3g) Subscribe to quotes
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "quotes": [VALID_SYMBOL]}},
        {"action": "recv"},
    ], "3g) Subscribe quotes — valid symbol")

    # 3h) Subscribe to multiple data types at once
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {
            "action": "subscribe",
            "trades": [VALID_SYMBOL],
            "quotes": [VALID_SYMBOL],
            "bars": [VALID_SYMBOL],
        }},
        {"action": "recv"},
    ], "3h) Subscribe trades+quotes+bars at once")

    # 3i) Subscribe with unknown field
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL], "bananas": ["fruit"]}},
        {"action": "recv"},
    ], "3i) Subscribe with unknown field (bananas)")

    # 3j) Double subscribe
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},
        {"action": "send", "data": {"action": "subscribe", "trades": ["QQQ"]}},
        {"action": "recv"},
    ], "3j) Double subscribe — second subscribe adds more symbols")

    # 3k) Unsubscribe
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL, "QQQ"]}},
        {"action": "recv"},
        {"action": "send", "data": {"action": "unsubscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},
    ], "3k) Unsubscribe from one symbol")


async def test_data_reception(api_key: str, secret_key: str) -> None:
    _section("4) DATA RECEPTION — capturing live messages")

    auth_steps = [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},
    ]

    # 4a) Subscribe to trades and receive data
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},  # subscription confirmation
        {"action": "recv_many", "count": 5, "timeout": DATA_WAIT_SEC},
    ], "4a) Receive trade messages (up to 5)", timeout=DATA_WAIT_SEC + 20)

    # 4b) Subscribe to quotes and receive data
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "quotes": [VALID_SYMBOL]}},
        {"action": "recv"},
        {"action": "recv_many", "count": 5, "timeout": DATA_WAIT_SEC},
    ], "4b) Receive quote messages (up to 5)", timeout=DATA_WAIT_SEC + 20)

    # 4c) Subscribe to bars and receive data
    await _connect_and_run("iex", auth_steps + [
        {"action": "send", "data": {"action": "subscribe", "bars": [VALID_SYMBOL]}},
        {"action": "recv"},
        {"action": "recv_many", "count": 3, "timeout": DATA_WAIT_SEC},
    ], "4c) Receive bar messages (up to 3, may be empty outside market hours)",
       timeout=DATA_WAIT_SEC + 20)


async def test_edge_cases(api_key: str, secret_key: str) -> None:
    _section("5) PROTOCOL EDGE CASES")

    # 5a) Double auth
    await _connect_and_run("iex", [
        {"action": "recv"},  # connected
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},  # authenticated
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},  # what happens on second auth?
    ], "5a) Double auth — auth twice after connected")

    # 5b) Auth with wrong creds, then correct creds
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": "pk_WRONG", "secret": "WRONG"}},
        {"action": "recv"},  # error?
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},  # can we retry?
    ], "5b) Auth fail then retry with correct creds")

    # 5c) Subscribe, unsubscribe all, check subscription state
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},
        {"action": "send", "data": {"action": "unsubscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},
    ], "5c) Subscribe then unsubscribe all — subscription state")

    # 5d) Send unknown action after auth
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},
        {"action": "send", "data": {"action": "foobar", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},
    ], "5d) Unknown action (foobar) after successful auth")

    # 5e) Send empty object
    await _connect_and_run("iex", [
        {"action": "recv"},
        {"action": "send", "data": {}},
        {"action": "recv"},
    ], "5e) Send empty object after connect")

    # 5f) SIP feed auth (may fail based on subscription tier)
    await _connect_and_run("sip", [
        {"action": "recv"},
        {"action": "send", "data": {"action": "auth", "key": api_key, "secret": secret_key}},
        {"action": "recv"},
        {"action": "send", "data": {"action": "subscribe", "trades": [VALID_SYMBOL]}},
        {"action": "recv"},
        {"action": "recv_many", "count": 3, "timeout": 5},
    ], "5f) SIP feed — auth + subscribe (may reject if not subscribed to SIP)",
       timeout=30)


async def test_alpaca_py_streaming(api_key: str, secret_key: str) -> None:
    """Test alpaca-py SDK streaming wrapper behavior."""
    _section("6) ALPACA-PY SDK STREAMING BEHAVIOR")

    try:
        from alpaca.data.live import StockDataStream
    except ImportError:
        print("  alpaca-py not installed — skipping SDK streaming tests")
        return

    # 6a) Create client with valid creds
    _case("6a) SDK StockDataStream — create with valid creds")
    try:
        client = StockDataStream(api_key=api_key, secret_key=secret_key)
        print(f"  Client created: {type(client).__name__}")
        print(f"  Client attrs: {[a for a in dir(client) if not a.startswith('_') and callable(getattr(client, a))]}")
    except Exception as exc:
        print(f"  Exception: {type(exc).__name__}: {exc}")

    # 6b) Create client with wrong creds
    _case("6b) SDK StockDataStream — create with wrong creds (no connect yet)")
    try:
        bad_client = StockDataStream(api_key="pk_WRONG", secret_key="WRONG")
        print(f"  Client created (no exception until connect): {type(bad_client).__name__}")
    except Exception as exc:
        print(f"  Exception on construction: {type(exc).__name__}: {exc}")

    # 6c) Subscribe and run briefly
    _case("6c) SDK — subscribe_trades + run for 8s")
    messages_received: list[dict[str, Any]] = []

    async def on_trade(trade: Any) -> None:
        msg = {
            "type": type(trade).__name__,
            "symbol": getattr(trade, "symbol", None),
            "price": getattr(trade, "price", None),
            "size": getattr(trade, "size", None),
            "timestamp": str(getattr(trade, "timestamp", None)),
        }
        messages_received.append(msg)
        if len(messages_received) <= 5:
            print(f"  << Trade: {msg}")

    try:
        client = StockDataStream(api_key=api_key, secret_key=secret_key)
        client.subscribe_trades(on_trade, VALID_SYMBOL)

        async def run_with_timeout() -> None:
            task = asyncio.create_task(client._run_forever())
            await asyncio.sleep(8)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        await run_with_timeout()
        print(f"  Total messages received: {len(messages_received)}")
    except Exception as exc:
        print(f"  Exception: {type(exc).__name__}: {exc}")
        traceback.print_exc()

    # 6d) SDK with wrong creds — subscribe and run (expect auth failure)
    _case("6d) SDK — wrong creds, subscribe + run")
    try:
        bad_client = StockDataStream(api_key="pk_WRONG_KEY", secret_key="WRONG_SECRET")
        bad_client.subscribe_trades(lambda t: None, VALID_SYMBOL)

        async def run_bad_with_timeout() -> None:
            task = asyncio.create_task(bad_client._run_forever())
            await asyncio.sleep(5)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception) as exc:
                print(f"  Task exception: {type(exc).__name__}: {exc}")

        await run_bad_with_timeout()
    except Exception as exc:
        print(f"  Exception: {type(exc).__name__}: {exc}")
        traceback.print_exc()


async def amain() -> None:
    api_key = _env("ALPACA_API_KEY")
    secret_key = _env("ALPACA_SECRET_KEY")

    print("Alpaca WebSocket Streaming — Behavior Probe")
    print(f"Timestamp: {datetime.now(tz=UTC).isoformat()}")
    print(f"WSS Base: {WSS_BASE}")
    print(f"Valid symbol: {VALID_SYMBOL}")
    print(f"Invalid symbol: {INVALID_SYMBOL}")
    print(f"Data wait: {DATA_WAIT_SEC}s")

    if not api_key or not secret_key:
        print("\nERROR: ALPACA_API_KEY and ALPACA_SECRET_KEY must both be set.")
        sys.exit(1)

    await test_connection()
    await test_auth(api_key, secret_key)
    await test_subscription(api_key, secret_key)
    await test_data_reception(api_key, secret_key)
    await test_edge_cases(api_key, secret_key)
    await test_alpaca_py_streaming(api_key, secret_key)

    _section("DONE")
    print("All streaming scenarios executed. Review output above for exact Alpaca behavior.")


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
