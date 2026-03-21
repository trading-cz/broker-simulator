"""Alpaca test-data store — loads JSONL files and indexes by symbol."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from simulator.common.io import load_jsonl

logger = logging.getLogger(__name__)


@dataclass
class AlpacaDataStore:
    """In-memory store for Alpaca test data, indexed by symbol."""

    bars: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    snapshots: dict[str, dict[str, Any]] = field(default_factory=dict)
    trades: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


def load_data(data_dir: str) -> AlpacaDataStore:
    """Load all JSONL test data from *data_dir*, indexed by symbol."""
    base = Path(data_dir)
    store = AlpacaDataStore()

    for record in load_jsonl(base / "bars_daily.jsonl"):
        store.bars.setdefault(record["symbol"], []).append(record)

    # Snapshots — keep latest per symbol (last entry wins)
    for record in load_jsonl(base / "snapshots.jsonl"):
        store.snapshots[record["symbol"]] = record

    for record in load_jsonl(base / "trades.jsonl"):
        store.trades.setdefault(record["symbol"], []).append(record)

    logger.info(
        "Data store ready: %d bar symbols, %d snapshot symbols, %d trade symbols",
        len(store.bars), len(store.snapshots), len(store.trades),
    )
    return store
