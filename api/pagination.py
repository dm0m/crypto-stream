"""Opaque cursor encoding for the keyset pagination of candle reads."""

import base64
import binascii
from datetime import datetime


def encode_cursor(ts: datetime) -> str:
    """Encode a bucket start as the opaque cursor string sent to clients."""
    raw = ts.isoformat().encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> datetime:
    """Decode a client-supplied cursor back into a bucket start.

    Raises:
        ValueError: The cursor is not valid base64, not valid UTF-8, not a
            valid ISO 8601 timestamp, or carries no timezone.
    """
    padded = cursor + "=" * (-len(cursor) % 4)
    try:
        ts = datetime.fromisoformat(base64.urlsafe_b64decode(padded).decode())
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"malformed cursor: {cursor!r}") from exc
    if ts.tzinfo is None:
        raise ValueError(f"cursor carries no timezone: {cursor!r}")
    return ts
