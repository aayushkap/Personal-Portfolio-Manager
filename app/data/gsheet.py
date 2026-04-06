# app/data/gsheet.py

import gspread
import os
import re

from app.config import ACCESS_DIR
from app.utils.time_utils import normalise_date


class GSheet_Manager:
    SERVICE_ACCOUNT_FILE = os.path.join(
        ACCESS_DIR,
        os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE"),
    )
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

    assert SERVICE_ACCOUNT_FILE, SPREADSHEET_ID

    def __init__(self):
        pass

    def fetch_transactions(self):
        try:
            gc = gspread.service_account(filename=self.SERVICE_ACCOUNT_FILE)
            sh = gc.open_by_key(self.SPREADSHEET_ID)
            worksheet = sh.get_worksheet(0)

            # Get values (for data) and formulas (for IMAGE cells) separately
            rows = worksheet.get_all_records()  # rendered values
            formula_rows = worksheet.get_all_values(
                value_render_option="FORMULA"
            )  # raw formulas

            return self.format_transactions(rows, formula_rows)
        except Exception:
            import traceback

            traceback.print_exc()

    def format_transactions(
        self,
        rows: list,
        formula_rows: list,
    ):
        logo_col_index = None
        headers = formula_rows[0] if formula_rows else []

        result = []

        # Find which column is "Logo"
        if "Logo" in headers:
            logo_col_index = headers.index("Logo")

        for i, row in enumerate(rows):
            clean_row = {}

            if logo_col_index is not None and i + 1 < len(formula_rows):
                formula_cell = formula_rows[i + 1][logo_col_index]  # +1 to skip header
                # Parse: =IMAGE("https://...") or =IMAGE(A1) or =IMAGE("url", mode)
                match = re.search(r'IMAGE\("([^"]+)"', formula_cell, re.IGNORECASE)
                if match:
                    row["logo_url"] = match.group(1)
                else:
                    row["logo_url"] = None
            else:
                row["logo_url"] = None

            # Split Exchange and Symbol
            if row.get("Symbol") and ":" in str(row.get("Symbol")):
                exchange, symbol = row["Symbol"].split(":", 1)
                row["Exchange"] = exchange
                row["Symbol"] = symbol

            if row.get("Logo", None) or row.get("Logo") == "":
                del row["Logo"]
            if (
                row.get("Next Expected Dividend Amount", None)
                or row.get("Next Expected Dividend Amount") == ""
            ):
                del row["Next Expected Dividend Amount"]
            if (
                row.get("Next Expected Dividend Date", None)
                or row.get("Next Expected Dividend Date") == ""
            ):
                del row["Next Expected Dividend Date"]

            for k, v in row.items():
                clean_row[k.replace(" ", "_").lower()] = v
            clean_row["purchase_date"] = normalise_date(clean_row.get("purchase_date"))
            result.append(clean_row)
        return result
