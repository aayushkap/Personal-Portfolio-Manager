# app/data/gsheet.py

import gspread
import os
import re
from contextlib import contextmanager
from typing import Iterator

from app.config import ACCESS_DIR
from app.data.ticker import TickerInfo, parse_ticker
from app.utils.time_utils import normalise_date


class WatchlistConflictError(Exception):
    pass


class WatchlistNotFoundError(Exception):
    pass


class GSheet_Manager:
    SERVICE_ACCOUNT_FILE = os.path.join(
        ACCESS_DIR, os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE")
    )
    SPREADSHEET_ID = os.getenv("TXN_SPREADSHEET_ID")
    WATCHLIST_GID = os.getenv("WATCHLIST_SPREADSHEET_ID")

    assert SERVICE_ACCOUNT_FILE, SPREADSHEET_ID

    def __init__(self):
        pass

    @contextmanager
    def _open_sheet(self) -> Iterator[object]:
        """Open a sheet and always release gspread's underlying HTTP session.

        ``gspread.service_account`` creates a new ``AuthorizedSession``.  The
        worker creates this manager repeatedly, so leaving that session open
        leaks its Google HTTPS sockets until the process reaches its file
        descriptor limit.
        """
        gc = gspread.service_account(filename=self.SERVICE_ACCOUNT_FILE)
        try:
            yield gc.open_by_key(self.SPREADSHEET_ID)
        finally:
            gc.http_client.session.close()

    def fetch_transactions(self) -> list[dict]:
        try:
            with self._open_sheet() as sh:
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
            with self._open_sheet() as sh:
                ws = self._worksheet_by_gid(sh, self.WATCHLIST_GID)
                values = ws.get_all_values()
                if not values:
                    return []
                headers = [h for h in values[0] if h]
                rows = [
                    dict(zip(headers, row)) for row in values[1:] if row and row[0]
                ]
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
            raw = str(row.get("Instrument", "")).strip().replace(" ", "")
            t = parse_ticker(raw)
            if not t:
                continue
            result.append(
                {
                    **_ticker_fields(t),
                    "note": str(row.get("Note", "")).strip() or None,
                    "criteria": str(row.get("Criteria", "")).strip() or None,
                    "tags": str(row.get("Tags", "")).strip() or None,
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

    WATCHLIST_HEADERS = ["Instrument", "Note", "Criteria", "Tags"]

    def _ticker_key(self, raw: str) -> str | None:
        cleaned = str(raw or "").strip().replace(" ", "")
        t = parse_ticker(cleaned)
        return t.key if t else None

    def _find_watchlist_row(
        self, ws, ticker_key: str
    ) -> tuple[int, list[str], list[str]]:
        """Fresh scan every call. Returns (row_idx, headers, row_values)."""
        values = ws.get_all_values()
        headers = values[0]
        instrument_idx = headers.index("Instrument")

        for i, row in enumerate(values[1:], start=2):
            if instrument_idx >= len(row):
                continue
            if self._ticker_key(row[instrument_idx]) == ticker_key:
                return i, headers, row

        return None, headers, None

    def upsert_watchlist_row(
        self,
        ticker: str,
        note: str | None = None,
        criteria: str | None = None,
        tags: list[str] | None = None,
    ) -> dict:
        t = parse_ticker(str(ticker).strip().replace(" ", ""))
        if not t:
            raise ValueError(f"Invalid ticker: {ticker}")

        with self._open_sheet() as sh:
            ws = self._worksheet_by_gid(sh, self.WATCHLIST_GID)

            row_idx, headers, current_row = self._find_watchlist_row(ws, t.key)

            if row_idx is None:
                # Create: fresh row, blank for anything not supplied
                instrument_str = f"{t.tv_exchange}:{t.tv_symbol}"
                row = [""] * len(headers)
                row[headers.index("Instrument")] = instrument_str
                if "Note" in headers:
                    row[headers.index("Note")] = note or ""
                if "Criteria" in headers:
                    row[headers.index("Criteria")] = criteria or ""
                if "Tags" in headers:
                    row[headers.index("Tags")] = _format_tags(tags)

                ws.append_row(row, value_input_option="USER_ENTERED")
                self._log_audit("watchlist_create", t.key, None, row)
                return {
                    "ticker": t.key,
                    "instrument": instrument_str,
                    "created": True,
                }

            # Update: only touch fields explicitly provided
            import gspread.utils as gutils

            fields: dict[str, str] = {}
            if note is not None:
                fields["Note"] = note
            if criteria is not None:
                fields["Criteria"] = criteria
            if tags is not None:
                fields["Tags"] = _format_tags(tags)

            batch = []
            for field, new_value in fields.items():
                if field not in headers:
                    continue
                col_idx = headers.index(field) + 1
                a1 = gutils.rowcol_to_a1(row_idx, col_idx)
                batch.append({"range": a1, "values": [[new_value]]})

            if batch:
                ws.batch_update(batch)
                self._log_audit("watchlist_update", t.key, current_row, fields)

            return {"ticker": t.key, "created": False}

    # Delete
    def delete_watchlist_row(self, ticker: str) -> None:
        t = parse_ticker(str(ticker).strip().replace(" ", ""))
        if not t:
            raise ValueError(f"Invalid ticker: {ticker}")

        with self._open_sheet() as sh:
            ws = self._worksheet_by_gid(sh, self.WATCHLIST_GID)

            row_idx, _, current_row = self._find_watchlist_row(ws, t.key)
            if row_idx is None:
                raise WatchlistNotFoundError(f"{t.key} not found on the watchlist")

            ws.delete_rows(row_idx)
            self._log_audit("watchlist_delete", t.key, current_row, None)

    def _log_audit(self, action: str, ticker: str, before, after) -> None:
        from app.core.logger import get_logger

        get_logger().info(
            "GSHEET_AUDIT action=%s ticker=%s before=%s after=%s",
            action,
            ticker,
            before,
            after,
        )


def _format_tags(tags: list[str] | None) -> str:
    if not tags:
        return ""
    cleaned = [t.strip().title() for t in tags if t and t.strip()]
    return " + ".join(cleaned)


def _ticker_fields(t: TickerInfo) -> dict:
    return {
        "ticker": t.key,
        "symbol": t.tv_symbol,
        "sa_symbol": t.sa_symbol,
        "exchange": t.tv_exchange,
        "sa_exchange": t.sa_exchange,
    }
