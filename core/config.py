"""Process configuration loaded from environment variables (12-factor style)."""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Connection and naming settings for Postgres, Redis and the stream consumers."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    db_name: str
    db_user: str
    db_password: SecretStr
    db_host: str
    db_port: int
    redis_host: str
    redis_port: int
    redis_password: SecretStr
    trade_worker_consumer: str
    trade_recovery_consumer: str
    candle_interval: str = "1m"
    candle_grace_seconds: float = Field(default=2.0, ge=0)

    @property
    def database_url(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password.get_secret_value()}@{self.db_host}:{self.db_port}/{self.db_name}"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # pyright: ignore[reportCallIssue]
