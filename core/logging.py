import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

import structlog
from structlog.contextvars import merge_contextvars


def configure_logging(
    log_file: str = "logs/app.log",
    level: int = logging.INFO,
    backup_count: int = 30,
) -> None:
    """Route structlog through stdlib logging to a daily-rotated JSON file."""
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Processors shared by both structlog-native and foreign (stdlib) records.
    shared_processors: list[Any] = [
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.CallsiteParameterAdder(
            [
                structlog.processors.CallsiteParameter.MODULE,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            ]
        ),
    ]

    structlog.configure(
        processors=[
            merge_contextvars,
            *shared_processors,
            # Hand off to stdlib logging instead of rendering here.
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # The stdlib handler does the final rendering (JSON).
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
    )

    handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        backupCount=backup_count,
        utc=True,
    )
    # Roll active "logs/app.log" -> "logs/app-YYYY-MM-DD.log" for the finished day.
    handler.suffix = "%Y-%m-%d"
    handler.namer = lambda name: name.replace("app.log.", "app-") + ".log"
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
