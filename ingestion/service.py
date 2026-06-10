import asyncio
import json
from io import TextIOWrapper
from typing import cast

import structlog
import websockets

from core.backoff import backoff
from core.log_events import LogEvent
from model.trade import Trade
from normalizer.binance import BinanceNormalizer
from model.enums import Exchange
from schemas.binance import BinanceTradeRaw

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


class IngestionService:
    """Orchestrates the Binance ingestion pipeline."""

    def __init__(
        self,
        symbols: list[str] | None = None,
        batch_size: int = 1000,
    ) -> None:
        self.symbols = symbols or ["btcusdt"]
        self.batch_size = batch_size
        self.queue: asyncio.Queue[Trade] = asyncio.Queue(maxsize=batch_size)
        self.received = 0
        self.logger: structlog.BoundLogger = structlog.get_logger().bind(
            service="ingestion", exchange=Exchange.BINANCE
        )

    async def run(self) -> None:
        self.logger.info(LogEvent.INGESTION_STARTED)
        asyncio.create_task(self._write())
        asyncio.create_task(self._report_metrics())
        await self._connect_and_consume()

    @backoff()
    async def _connect_and_consume(self) -> None:
        async with websockets.connect(BINANCE_WS_URL) as ws:
            subscribe_msg = json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": [f"{s}@trade" for s in self.symbols],
                    "id": 1,
                }
            )
            await ws.send(subscribe_msg)
            await self._receive(ws)

    async def _receive(self, ws: websockets.WebSocketClientProtocol) -> None:
        self.logger.info(LogEvent.WS_CONNECTED, symbols=self.symbols)
        async for message in ws:
            raw_trade: BinanceTradeRaw = cast(BinanceTradeRaw, json.loads(message))
            trade = BinanceNormalizer.normalize(raw_trade)
            if trade is not None:
                await self.queue.put(trade)
                self.received += 1

    async def _write(self) -> None:
        # TODO(step 2): replace the jsonl sink with a storage repository bulk-insert.
        with open("data.jsonl", "a") as f:
            while True:
                trade = await self.queue.get()
                batch: list[Trade] = [trade]
                while len(batch) < self.batch_size:
                    try:
                        batch.append(self.queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                await asyncio.to_thread(self._flush, f, batch)

    @staticmethod
    def _flush(file: TextIOWrapper, trades: list[Trade]) -> None:
        for trade in trades:
            file.write(f"{trade.model_dump_json()}\n")

    async def _report_metrics(self, interval: int = 5) -> None:
        dropped_prev = 0
        received_prev = 0
        while True:
            await asyncio.sleep(interval)
            dropped_now = BinanceNormalizer.malformed_trade_count
            dropped_delta = dropped_now - dropped_prev
            received_now = self.received
            received_delta = received_now - received_prev

            drop_ratio = (
                dropped_delta / received_delta if received_delta > 0 else 0.0
            )
            queue_pct = self.queue.qsize() / self.queue.maxsize * 100

            self.logger.info(
                LogEvent.METRICS_SNAPSHOT,
                dropped_total=dropped_now,
                dropped_delta=dropped_delta,
                dropped_per_sec=dropped_delta / interval,
                received_per_sec=received_delta / interval,
                drop_ratio=drop_ratio,
                queue_pct=queue_pct,
            )
            dropped_prev = dropped_now
            received_prev = received_now
