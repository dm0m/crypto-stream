from enum import StrEnum


class LogEvent(StrEnum):
    """Every structured log event name the services emit, in one place."""

    INGESTION_STARTED = "ingestion.started"
    WS_CONNECTED = "ws.connected"
    WS_RECONNECTING = "ws.reconnecting"
    REDIS_CONNECTED = "redis.connected"
    REDIS_RECONNECTING = "redis.reconnecting"
    WS_ACK_RECEIVED = "ws.ack_received"
    TRADE_DROPPED = "trade.dropped"
    METRICS_SNAPSHOT = "metrics.snapshot"
    PROCESSING_STARTED = "processing.started"
    PROCESSING_BATCH = "processing.batch"
    PROCESSING_ERROR = "processing.error"
    PROCESSING_CONSUMER_PRUNED = "processing.consumer_pruned"
    SHUTDOWN_INITIATED = "shutdown.initiated"
    SHUTDOWN_COMPLETE = "shutdown.complete"
    DLQ_NOT_EMPTY = "dlq.not_empty"
    DB_RECONNECTING = "psql.reconnecting"
    AGGREGATOR_CANDLE_CLOSED = "aggregator.candle_closed"
    AGGREGATOR_LATE_TRADE = "aggregator.late_trade"
    CACHE_HIT = "cache.hit"
    CACHE_MISS = "cache.miss"
    CACHE_UNAVAILABLE = "cache.unavailable"
    CACHE_RECOVERED = "cache.recovered"
    HEALTH_CHECK_FAILED = "health.check_failed"
