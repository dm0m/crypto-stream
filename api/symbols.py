"""Rejects requests for trading pairs the API does not serve."""

from fastapi import HTTPException, status

from api.settings import ApiSettings
from domain.enums import Exchange


def ensure_known_symbol(exchange: Exchange, symbol: str, settings: ApiSettings) -> None:
    """Raise a 404 unless ``symbol`` is served on ``exchange``.

    Raises:
        HTTPException: 404 when the pair is not served on that exchange; the
            detail lists the pairs that are.
    """
    known = settings.symbols.get(exchange, frozenset())
    if symbol not in known:
        listed = ", ".join(sorted(known)) or "none"
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown symbol {symbol} on {exchange}; served: {listed}",
        )
