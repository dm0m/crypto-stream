"""Unit tests for the opaque pagination cursor."""

import base64
from datetime import UTC, datetime, timedelta, timezone

import pytest
from hypothesis import given
from hypothesis import strategies as st

from api.pagination import decode_cursor, encode_cursor

TS = datetime(2026, 9, 15, 12, 34, 56, 789012, tzinfo=UTC)

# Offsets across the full real-world range, which is wider than -12..+12.
_offsets = st.integers(min_value=-14 * 60, max_value=14 * 60).map(
    lambda minutes: timezone(timedelta(minutes=minutes))
)
_aware_datetimes = st.datetimes(
    min_value=datetime(2000, 1, 1),
    max_value=datetime(2100, 1, 1),
    timezones=_offsets,
)


def test_round_trip_preserves_the_instant() -> None:
    assert decode_cursor(encode_cursor(TS)) == TS


def test_round_trip_preserves_microseconds() -> None:
    assert decode_cursor(encode_cursor(TS)).microsecond == 789012


@given(ts=_aware_datetimes)
def test_any_aware_datetime_round_trips(ts: datetime) -> None:
    assert decode_cursor(encode_cursor(ts)) == ts


@given(ts=_aware_datetimes)
def test_encoded_cursor_is_url_safe(ts: datetime) -> None:
    """Cursors travel in query strings, so no character may need escaping."""
    encoded = encode_cursor(ts)
    assert not set(encoded) & set("+/=")


def test_offsets_are_preserved_as_the_same_instant() -> None:
    """A caller may mint a cursor in any zone and still page correctly."""
    berlin = datetime(
        2026, 9, 15, 14, 34, 56, 789012, tzinfo=timezone(timedelta(hours=2))
    )
    assert decode_cursor(encode_cursor(berlin)) == TS


@pytest.mark.parametrize(
    ("cursor", "reason"),
    [
        pytest.param("not-base64!!", "invalid base64 alphabet", id="bad_alphabet"),
        pytest.param("", "empty string decodes to no timestamp", id="empty"),
        pytest.param("////", "decodes to bytes that are not UTF-8", id="not_utf8"),
        pytest.param("aGVsbG8", "valid text that is not a timestamp", id="not_a_date"),
    ],
)
def test_malformed_cursors_raise_value_error(cursor: str, reason: str) -> None:
    with pytest.raises(ValueError, match="malformed cursor"):
        decode_cursor(cursor)


def test_naive_cursor_is_rejected() -> None:
    """A timestamp with no offset would be read in the server's local zone."""
    encoded = base64.urlsafe_b64encode(b"2026-09-15T12:34:56").decode().rstrip("=")
    with pytest.raises(ValueError, match="no timezone"):
        decode_cursor(encoded)


@given(ts=_aware_datetimes)
def test_padding_is_restored_for_every_length(ts: datetime) -> None:
    assert decode_cursor(encode_cursor(ts)) == ts
