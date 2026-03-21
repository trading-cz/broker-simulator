"""Common I/O utilities — reusable across all broker implementations."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load a JSON-lines file into a list of dicts.

    Returns an empty list if the file does not exist.
    """
    if not path.exists():
        logger.warning("Data file not found: %s", path)
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").strip().splitlines():
        if line.strip():
            records.append(json.loads(line))
    logger.info("Loaded %d records from %s", len(records), path.name)
    return records
