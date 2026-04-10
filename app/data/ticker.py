# app/data/ticker.py  — only parse_ticker changes

from dataclasses import dataclass


@dataclass(frozen=True)
class TickerInfo:
    raw: str
    tv_exchange: str
    sa_exchange: str
    tv_symbol: str
    sa_symbol: str

    @property
    def key(self) -> str:
        """Canonical DB/cache key — always TV format."""
        return f"{self.tv_exchange}:{self.tv_symbol}"


def parse_ticker(raw: str) -> "TickerInfo | None":
    raw = raw.strip().upper()
    if ":" not in raw:
        return None

    exchange_part, symbol_part = raw.split(":", 1)

    tv_exchange, sa_exchange = (
        exchange_part.split("/", 1)
        if "/" in exchange_part
        else (exchange_part, exchange_part)
    )

    tv_symbol, sa_symbol = (
        symbol_part.split("/", 1) if "/" in symbol_part else (symbol_part, symbol_part)
    )

    if not tv_exchange or not tv_symbol:
        return None

    return TickerInfo(
        raw=raw,
        tv_exchange=tv_exchange,
        sa_exchange=sa_exchange,
        tv_symbol=tv_symbol,
        sa_symbol=sa_symbol,
    )
