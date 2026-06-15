from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from functools import lru_cache

class Settings(BaseSettings): 
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    
    db_name: str
    db_user: str
    db_password: SecretStr
    db_host: str
    db_port: int
                
    @property
    def database_url(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password.get_secret_value()}@{self.db_host}:{self.db_port}/{self.db_name}"


@lru_cache
def get_settings() -> Settings:
        return Settings() # pyright: ignore[reportCallIssue]
    
