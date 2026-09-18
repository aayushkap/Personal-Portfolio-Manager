import unittest
from datetime import date

from app.data.gsheet import GSheet_Manager
from app.hql.queries.portfolio import PortfolioQuery


class _CacheRepository:
    def __init__(self, raw):
        self.raw = raw

    def list_tickers(self):
        return ["NYSE:MCD"]

    def get_raw_ticker(self, ticker):
        return self.raw

    def resolve_currency(self, raw):
        return "USD"


class _FX:
    def to_aed(self, value, currency):
        return value


def _query(receipts):
    return PortfolioQuery(
        _CacheRepository(
            {
                "purchase_details": [
                    {
                        "purchase_date": "2026-08-01",
                        "transaction": "BUY",
                        "shares": "3",
                        "cost_per_share": "USD 250",
                        "total_cost": "USD 750",
                    }
                ],
                "dividends": {
                    "rows": [
                        {
                            "Ex-Dividend Date": "2026-09-01",
                            "Pay Date": "2026-09-15",
                            "Cash Amount": "USD 1.86",
                        }
                    ]
                },
            }
        ),
        price_repo=None,
        fx=_FX(),
        confirmed_dividends_loader=lambda: receipts,
    )


class ConfirmedDividendTests(unittest.TestCase):
    def test_confirmed_receipt_replaces_expected_gross_with_net_cash(self):
        divs = _query(
            [
                {
                    "ticker": "NYSE:MCD",
                    "received_date": "2026-09-17",
                    "shares_held": "3",
                    "dividend_per_share": "USD 1.86",
                    "total_dividend": "USD 5.58",
                    "tax_paid": "USD 1.67",
                }
            ]
        ).dividends(on=date(2026, 9, 17))

        self.assertEqual(
            list(divs.columns),
            [
                "ticker",
                "ex_date",
                "pay_date",
                "shares_held",
                "amount_per_share_aed",
                "total_aed",
                "status",
            ],
        )
        row = divs.iloc[0]
        self.assertEqual(row["status"], "received")
        self.assertEqual(row["ex_date"], date(2026, 9, 1))
        self.assertEqual(row["pay_date"], date(2026, 9, 17))
        self.assertEqual(row["total_aed"], 3.91)

    def test_past_scraped_payment_stays_pending_without_confirmation(self):
        divs = _query([]).dividends(on=date(2026, 9, 17))

        self.assertEqual(divs.iloc[0]["status"], "pending")
        self.assertEqual(divs.iloc[0]["total_aed"], 5.58)

    def test_unmatched_confirmed_receipt_is_still_received(self):
        divs = _query(
            [
                {
                    "ticker": "NYSE:MCD",
                    "received_date": "2026-08-05",
                    "shares_held": "3",
                    "dividend_per_share": "USD 1.00",
                    "total_dividend": "USD 3.00",
                    "tax_paid": "USD 0.50",
                }
            ]
        ).dividends(on=date(2026, 9, 17))

        received = divs[divs["status"] == "received"].iloc[0]
        self.assertEqual(received["pay_date"], date(2026, 8, 5))
        self.assertEqual(received["total_aed"], 2.5)

    def test_dividend_sheet_normalizes_alias_exchange(self):
        rows = GSheet_Manager._format_confirmed_dividends(
            [
                {
                    "Date Received": "2026-08-28",
                    "Symbol": "LSE/LON:UKW",
                    "Shares Held": 400,
                    "Dividend per Share": "GBP 0.03",
                    "Total Dividend": "GBP 10.72",
                    "Tax Paid": "GBP 0.00",
                }
            ]
        )

        self.assertEqual(rows[0]["ticker"], "LSE:UKW")


if __name__ == "__main__":
    unittest.main()
