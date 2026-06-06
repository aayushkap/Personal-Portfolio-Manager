# app/hql/facade.py

from __future__ import annotations

from app.data.cache import Cache
from app.data.db import DB
from app.hql.queries.ticker import TickerQuery, TickersQuery
from app.hql.queries.portfolio import PortfolioQuery
from app.hql.queries.watchlist import WatchlistQuery
from app.hql.repositories import CacheRepository, FXService, PriceRepository


class HQL:
    """
    HQL facade.

    Public entry points:
        hql.ticker("EXCHANGE:SYMBOL")
        hql.ticker("DFM:EMAAR")
        hql.tickers(["DFM:EMAAR", "NYSE:O"])
        hql.portfolio()
    """

    def __init__(self) -> None:
        self._cache = Cache()
        self._db = DB()

        self.fx = FXService()
        self.cache_repo = CacheRepository(self._cache)
        self.price_repo = PriceRepository(self._db, self.fx, self.cache_repo)

    def ticker(self, ticker: str) -> TickerQuery:
        return TickerQuery(
            ticker=ticker,
            cache_repo=self.cache_repo,
            price_repo=self.price_repo,
            fx=self.fx,
        )

    def tickers(self, tickers: list[str]) -> TickersQuery:
        return TickersQuery(
            tickers=tickers,
            cache_repo=self.cache_repo,
            price_repo=self.price_repo,
            fx=self.fx,
        )

    def portfolio(self) -> PortfolioQuery:
        return PortfolioQuery(
            cache_repo=self.cache_repo,
            price_repo=self.price_repo,
            fx=self.fx,
        )

    def watchlist(self) -> WatchlistQuery:
        return WatchlistQuery(
            cache_repo=self.cache_repo,
            price_repo=self.price_repo,
            fx=self.fx,
        )
