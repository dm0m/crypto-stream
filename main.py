import asyncio

from core.logging import configure_logging
from ingestion.service import IngestionService

configure_logging()


async def main() -> None:
    service = IngestionService()
    await service.run()


asyncio.run(main())
