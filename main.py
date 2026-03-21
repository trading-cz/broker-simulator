"""Broker simulator — entry point.

Starts a FastAPI server that emulates a broker's REST + WebSocket APIs,
serving test data from JSONL files.  The real alpaca-py SDK connects here
via ``url_override`` — production code path stays untouched.
"""

from __future__ import annotations

import asyncio
import logging

import uvicorn
from fastapi import FastAPI

from simulator.settings import SimulatorSettings

logger = logging.getLogger(__name__)


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    )


def _build_app(settings: SimulatorSettings) -> FastAPI:
    """Build the FastAPI application for the configured broker."""
    match settings.broker:
        case "alpaca":
            from simulator.alpaca.data_store import load_data
            from simulator.alpaca.rest import create_router
            from simulator.alpaca.wss import create_ws_router

            store = load_data(settings.broker_data_dir)
            app = FastAPI(title=f"Broker Simulator ({settings.broker})")
            app.include_router(create_router(store))
            app.include_router(create_ws_router(store, settings.replay_interval))
            return app
        case _:
            raise ValueError(f"Unsupported broker: {settings.broker}")


def main() -> None:
    """Application entry point."""
    _setup_logging()
    settings = SimulatorSettings()
    logger.info(
        "Starting broker simulator (broker=%s, port=%d, data=%s)",
        settings.broker, settings.port, settings.data_dir,
    )
    app = _build_app(settings)
    asyncio.run(_serve(app, settings))


async def _serve(app: FastAPI, settings: SimulatorSettings) -> None:
    config = uvicorn.Config(
        app, host=settings.host, port=settings.port, log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    main()
