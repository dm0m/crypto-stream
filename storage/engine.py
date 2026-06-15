from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from core.config import get_settings

db_url = get_settings().database_url
engine = create_async_engine(db_url, pool_size=10, max_overflow=20)
session_factory = async_sessionmaker(engine, expire_on_commit=False)
