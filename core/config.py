"""Configuration shared by more than one service, read from the environment."""

from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Connection details for Postgres, from ``DB_NAME``, ``DB_USER`` and so on."""

    model_config = SettingsConfigDict(
        env_prefix="DB_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    name: str
    user: str
    password: SecretStr
    host: str
    port: int

    @property
    def url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class RedisSettings(BaseSettings):
    """Connection details for Redis, from ``REDIS_HOST``, ``REDIS_PORT`` and so on."""

    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str
    port: int
    password: SecretStr


@lru_cache
def get_database_settings() -> DatabaseSettings:
    return DatabaseSettings()  # pyright: ignore[reportCallIssue]


@lru_cache
def get_redis_settings() -> RedisSettings:
    return RedisSettings()  # pyright: ignore[reportCallIssue]
