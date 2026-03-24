# portfolio_snapshot.py

import csv
import os
from datetime import datetime, date
from typing import Optional
import json

from time_utils import dubai_today

SNAPSHOT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "cache", "portfolio_snapshots.csv"
)

COLUMNS = [
    "date",
    "total_market_value_aed",  # current market value of all holdings
    "total_invested_aed",  # cumulative cost basis (all purchases ever)
    "daily_cash_flow_aed",  # new capital added THIS day (new purchases)
    "unrealized_pnl_aed",  # market_value - total_invested
    "unrealized_pnl_pct",  # unrealized_pnl / total_invested
    "twr_factor",  # running TWR chain-link factor (product)
    "twr_pct",  # (twr_factor - 1) * 100
]


class PortfolioSnapshotter:
    """
    Appends one row per day to portfolio_snapshots.csv.
    Calculates Time-Weighted Return (TWR) which neutralises new capital injections.

    TWR sub-period return formula:
        HP = (End Value - (Start Value + Cash Flow)) / (Start Value + Cash Flow)
        TWR = product(1 + HP_i) - 1
    """

    def __init__(self, path: str = SNAPSHOT_FILE):
        self.path = path
        self._init_file()

    def _init_file(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writeheader()

    def _load_all(self) -> list[dict]:
        rows = []
        with open(self.path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

    def _last_row(self) -> Optional[dict]:
        rows = self._load_all()
        return rows[-1] if rows else None

    def record(
        self,
        market_value: float,
        total_invested: float,
        cash_flow_today: float = 0.0,
        snapshot_date: Optional[date] = None,
    ) -> dict:
        """
        Call this once per day after fetching current prices.
        cash_flow_today = AED value of NEW purchases made today (0 if no new buys).
        """
        today = snapshot_date or dubai_today()
        today_str = today.isoformat()

        # Get all rows and find the previous record (excluding today if it exists)
        all_rows = self._load_all()
        prev_row = None
        for row in reversed(all_rows):
            if row["date"] != today_str:
                prev_row = row
                break

        prev_value = (
            float(prev_row["total_market_value_aed"]) if prev_row else market_value
        )
        prev_factor = float(prev_row["twr_factor"]) if prev_row else 1.0

        # TWR sub-period: neutralise today's cash injection in denominator
        # HP = (end - (start + cashflow)) / (start + cashflow)
        denominator = prev_value + cash_flow_today
        hp = (market_value - denominator) / denominator if denominator else 0.0

        twr_factor = prev_factor * (1 + hp)
        twr_pct = (twr_factor - 1) * 100

        unrealized_pnl = market_value - total_invested
        unrealized_pnl_pct = (
            (unrealized_pnl / total_invested * 100) if total_invested else 0.0
        )

        row = {
            "date": today_str,
            "total_market_value_aed": round(market_value, 2),
            "total_invested_aed": round(total_invested, 2),
            "daily_cash_flow_aed": round(cash_flow_today, 2),
            "unrealized_pnl_aed": round(unrealized_pnl, 2),
            "unrealized_pnl_pct": round(unrealized_pnl_pct, 4),
            "twr_factor": round(twr_factor, 8),
            "twr_pct": round(twr_pct, 4),
        }

        # Remove any existing record for today and write all rows back
        filtered_rows = [r for r in all_rows if r["date"] != today_str]
        with open(self.path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(filtered_rows)
            writer.writerow(row)

        print(
            f"  [snapshot] {today_str} | Value: {market_value:,.2f} | TWR: {twr_pct:+.2f}%"
        )
        return row

    def load_history(self) -> list[dict]:
        return self._load_all()

    def get_window(self, days: int) -> list[dict]:
        rows = self._load_all()
        return rows[-days:] if len(rows) >= days else rows
