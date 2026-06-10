from enum import StrEnum


class LogEvent(StrEnum):
    INGESTION_STARTED = "ingestion.started"
    WS_CONNECTED = "ws.connected"
    WS_RECONNECTING = "ws.reconnecting"
    WS_ACK_RECEIVED = "ws.ack_received"
    TRADE_DROPPED = "trade.dropped"
    METRICS_SNAPSHOT = "metrics.snapshot"
