# app/hql/queries/risk.py

from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252


class RiskQuery:
    """
    Stateless risk/reward statistics computed from daily return series.

    Unlike PortfolioQuery, this class owns no data access -- every method
    takes already-fetched pandas data (see PortfolioQuery.returns_matrix) and
    is pure math, so it's safe to reuse across positions without re-fetching
    anything.
    """

    @staticmethod
    def volatility_annualized(returns: pd.Series) -> float | None:
        """Annualized volatility: daily std dev x sqrt(252)."""
        returns = returns.dropna()
        if len(returns) < 2:
            return None
        return float(returns.std(ddof=1) * np.sqrt(TRADING_DAYS_PER_YEAR))

    @staticmethod
    def max_drawdown(prices: pd.Series) -> float | None:
        """Deepest peak-to-trough decline over the series, as a negative fraction."""
        prices = prices.dropna()
        if prices.empty:
            return None
        running_max = prices.cummax()
        drawdown = (prices - running_max) / running_max
        return float(drawdown.min())

    @staticmethod
    def beta(asset_returns: pd.Series, benchmark_returns: pd.Series) -> float | None:
        """Cov(asset, benchmark) / Var(benchmark), pairwise-aligned by date."""
        if asset_returns is None or benchmark_returns is None:
            return None
        aligned = pd.concat(
            [asset_returns.rename("asset"), benchmark_returns.rename("benchmark")],
            axis=1,
            join="inner",
        ).dropna()
        if len(aligned) < 2:
            return None
        variance = aligned["benchmark"].var(ddof=1)
        if not variance:
            return None
        covariance = aligned["asset"].cov(aligned["benchmark"])
        return float(covariance / variance)

    @staticmethod
    def downside_deviation_annualized(
        returns: pd.Series, minimum_acceptable_return: float = 0.0
    ) -> float | None:
        """Annualized std dev of returns falling below the minimum acceptable return."""
        returns = returns.dropna()
        if returns.empty:
            return None
        downside = (
            returns[returns < minimum_acceptable_return] - minimum_acceptable_return
        )
        if downside.empty:
            return 0.0
        return float(np.sqrt((downside**2).mean()) * np.sqrt(TRADING_DAYS_PER_YEAR))

    @staticmethod
    def sharpe_ratio(
        annualized_return: float | None,
        annualized_volatility: float | None,
        risk_free_rate: float = 0.0,
    ) -> float | None:
        if annualized_return is None or not annualized_volatility:
            return None
        return float((annualized_return - risk_free_rate) / annualized_volatility)

    @staticmethod
    def sortino_ratio(
        annualized_return: float | None,
        downside_deviation_annualized: float | None,
        risk_free_rate: float = 0.0,
    ) -> float | None:
        if annualized_return is None or not downside_deviation_annualized:
            return None
        return float(
            (annualized_return - risk_free_rate) / downside_deviation_annualized
        )

    @staticmethod
    def covariance_matrix_annualized(returns: pd.DataFrame) -> pd.DataFrame:
        """Pairwise annualized covariance matrix (252 trading days/year)."""
        if returns.empty:
            return pd.DataFrame()
        return returns.cov() * TRADING_DAYS_PER_YEAR

    @staticmethod
    def risk_contribution(
        weights: pd.Series, covariance_annualized: pd.DataFrame
    ) -> pd.Series:
        """
        Euler allocation of portfolio volatility:
            contribution_i = w_i * (Cov @ w)_i / sigma_portfolio
        Contributions sum exactly to the portfolio's annualized volatility.
        """
        if covariance_annualized.empty:
            return pd.Series(dtype=float)

        tickers = [t for t in weights.index if t in covariance_annualized.index]
        if not tickers:
            return pd.Series(dtype=float)

        w = weights.reindex(tickers).fillna(0.0)
        cov = covariance_annualized.reindex(index=tickers, columns=tickers).fillna(0.0)

        portfolio_variance = float(w.values @ cov.values @ w.values)
        if portfolio_variance <= 0:
            return pd.Series(0.0, index=tickers)

        portfolio_vol = np.sqrt(portfolio_variance)
        marginal = cov.values @ w.values
        contribution = w.values * marginal / portfolio_vol
        return pd.Series(contribution, index=tickers)

    @staticmethod
    def portfolio_volatility_annualized(
        weights: pd.Series, covariance_annualized: pd.DataFrame
    ) -> float | None:
        if covariance_annualized.empty:
            return None
        tickers = [t for t in weights.index if t in covariance_annualized.index]
        if not tickers:
            return None
        w = weights.reindex(tickers).fillna(0.0)
        cov = covariance_annualized.reindex(index=tickers, columns=tickers).fillna(0.0)
        variance = float(w.values @ cov.values @ w.values)
        return float(np.sqrt(variance)) if variance > 0 else None
