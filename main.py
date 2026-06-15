import asyncio
from storage.repositories import TradeRepository

from core.logging import configure_logging
from ingestion.service import IngestionService
from storage.engine import session_factory

configure_logging()


async def main() -> None:
    service = IngestionService(TradeRepository(session_factory))
    await service.run()

asyncio.run(main())
