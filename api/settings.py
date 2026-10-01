"""Settings only the HTTP API reads."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from domain.enums import Exchange


class ApiSettings(BaseSettings):
    """Where the API listens and how large a page of results may be."""

    model_config = SettingsConfigDict(
        env_prefix="API_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    host: str = "127.0.0.1"
    port: int = 8080
    default_page_size: int = Field(default=100, ge=1)
    max_page_size: int = Field(default=1000, ge=1)
    cache_ttl_live_seconds: int = Field(default=5, ge=1)
    cache_ttl_closed_seconds: int = Field(default=600, ge=1)
    cache_settle_seconds: int = Field(default=60, ge=0)
    symbols: dict[Exchange, frozenset[str]] = Field(
        default_factory=lambda: {Exchange.BINANCE: frozenset({"BTC-USDT"})}
    )


@lru_cache
def get_api_settings() -> ApiSettings:
    return ApiSettings()
