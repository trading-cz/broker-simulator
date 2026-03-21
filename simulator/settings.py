"""Simulator settings — loaded from environment variables."""

from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class SimulatorSettings(BaseSettings):
    """Top-level simulator configuration.

    Environment variables use ``SIM_`` prefix::

        SIM_BROKER=alpaca
        SIM_DATA_DIR=../ingestion/tests/data
        SIM_HOST=0.0.0.0
        SIM_PORT=8080
        SIM_REPLAY_INTERVAL=1.0
    """

    model_config = SettingsConfigDict(env_prefix="SIM_", extra="ignore")

    broker: Literal["alpaca"] = "alpaca"
    data_dir: str = "./data"
    host: str = "0.0.0.0"
    port: int = 8080
    replay_interval: float = 1.0

    @property
    def broker_data_dir(self) -> str:
        """Data directory for the active broker: ``{data_dir}/{broker}``."""
        return f"{self.data_dir}/{self.broker}"
