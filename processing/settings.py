"""Settings only the processing worker reads."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """Consumer identities and candle aggregation for ``python -m processing``."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    trade_worker_consumer: str
    trade_recovery_consumer: str
    candle_interval: str = "1m"
    candle_grace_seconds: float = Field(default=2.0, ge=0)


@lru_cache
def get_worker_settings() -> WorkerSettings:
    return WorkerSettings()  # pyright: ignore[reportCallIssue]
