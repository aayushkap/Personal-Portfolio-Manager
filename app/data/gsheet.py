# app/data/gsheet.py

import gspread
import os
import re

from app.config import ACCESS_DIR
from app.data.ticker import TickerInfo, parse_ticker
from app.utils.time_utils import normalise_date


class GSheet_Manager:
    SERVICE_ACCOUNT_FILE = os.path.join(
        ACCESS_DIR, os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE")
    )
    SPREADSHEET_ID = os.getenv("TXN_SPREADSHEET_ID")
    WATCHLIST_GID = os.getenv("WATCHLIST_SPREADSHEET_ID")

    assert SERVICE_ACCOUNT_FILE, SPREADSHEET_ID

    def __init__(self):
        pass

    def _open_sheet(self):
        gc = gspread.service_account(filename=self.SERVICE_ACCOUNT_FILE)
        return gc.open_by_key(self.SPREADSHEET_ID)

    def fetch_transactions(self) -> list[dict]:
        try:
            sh = self._open_sheet()
            worksheet = sh.get_worksheet(0)
            rows = worksheet.get_all_records()
            formula_rows = worksheet.get_all_values(value_render_option="FORMULA")
            return self.format_transactions(rows, formula_rows)
        except Exception:
            import traceback

            traceback.print_exc()
            return []

    def fetch_watchlist(self) -> list[dict]:
        try:
            sh = self._open_sheet()
            ws = self._worksheet_by_gid(sh, self.WATCHLIST_GID)
            values = ws.get_all_values()
            if not values:
                return []
            headers = [h for h in values[0] if h]
            rows = [dict(zip(headers, row)) for row in values[1:] if row and row[0]]
            return self._format_watchlist(rows)
        except Exception:
            import traceback

            traceback.print_exc()
            return []

    def _worksheet_by_gid(self, sh, gid: str):
        for ws in sh.worksheets():
            if str(ws.id) == gid:
                return ws
        raise ValueError(f"Worksheet gid={gid} not found")

    def _format_watchlist(self, rows: list) -> list[dict]:
        result = []
        for row in rows:
            raw = str(row.get("Instrument", "")).strip()
            t = parse_ticker(raw)
            if not t:
                continue
            result.append(
                {
                    **_ticker_fields(t),
                    "notes": str(row.get("Notes", "")).strip() or None,
                }
            )
        return result

    def format_transactions(self, rows: list, formula_rows: list) -> list[dict]:
        headers = formula_rows[0] if formula_rows else []
        logo_col_index = headers.index("Logo") if "Logo" in headers else None
        result = []

        for i, row in enumerate(rows):
            # Logo URL from IMAGE formula
            if logo_col_index is not None and i + 1 < len(formula_rows):
                formula_cell = formula_rows[i + 1][logo_col_index]
                match = re.search(r'IMAGE\("([^"]+)"', formula_cell, re.IGNORECASE)
                row["logo_url"] = match.group(1) if match else None
            else:
                row["logo_url"] = None

            # Parse exchange/symbol — supports EURONEXT/EPA:AI format
            raw = str(row.get("Symbol", "")).strip()
            t = parse_ticker(raw)
            if t:
                row["Exchange"] = t.tv_exchange
                row["Symbol"] = t.tv_symbol
                row.update(_ticker_fields(t))

            for drop in [
                "Logo",
                "Next Expected Dividend Amount",
                "Next Expected Dividend Date",
            ]:
                row.pop(drop, None)

            clean_row = {k.replace(" ", "_").lower(): v for k, v in row.items()}
            clean_row["purchase_date"] = normalise_date(clean_row.get("purchase_date"))
            result.append(clean_row)

        return result


def _ticker_fields(t: TickerInfo) -> dict:
    return {
        "ticker": t.key,
        "symbol": t.tv_symbol,
        "sa_symbol": t.sa_symbol,
        "exchange": t.tv_exchange,
        "sa_exchange": t.sa_exchange,
    }
