from __future__ import annotations

import json
from datetime import date, datetime
from typing import Optional

from google import genai
from google.genai import types
from app.core.logger import get_logger
from app.config import CACHE_DIR, GEMINI_KEY

logger = get_logger()

ALERTS_PATH = CACHE_DIR / "watchlist_alerts.json"

_EVALUATOR_PROMPT = """\
You are monitoring a stock watchlist. The stock has NOT been purchased.
Your only job is to assess whether the buy conditions have been met.
You have access to Google Search — use it to find current, specific information
for each condition. Do not evaluate from memory alone.

Ticker: {ticker}
Analyst thesis: {note}

Critical conditions (ALL must be met for ready_to_buy = true):
{critical_conditions}

Bonus conditions (nice to have, do NOT affect ready_to_buy):
{bonus_conditions}

Current data (from internal systems):
- Price: {price}
- Fundamentals: {fundamentals}

Today: {today}

Instructions:
- Search specifically for each condition. Do not batch them into one search.
- For price conditions, use the current price above — do not search for it.
- For macro conditions (e.g. 10-year Treasury, UK base rate), search for the
  latest value.
- One short, direct sentence per condition note. No essays. No filler.
- Set ready_to_buy = true ONLY if every critical condition is met.

Return your response as a JSON object wrapped in a ```json code block. \
Use this exact structure:
```json
{{
  "ready_to_buy": false,
  "critical_conditions": [{{"condition": "...", "met": false, "note": "..."}}],
  "bonus_conditions": [{{"condition": "...", "met": false, "note": "..."}}]
}}
"""


def _parse_conditions(criteria: str) -> tuple[list[str], list[str]]:
    """
    Split raw criteria text into critical and bonus condition lists.
    Handles both "Critical (Must-Haves)" / "Good to Have (Bonuses)" and
    "Must Have" / "Good to Have" / "Critical conditions" heading variants.
    """
    import re

    critical_headers = re.compile(
        r"(?i)(critical\s*(conditions?|must.?haves?)?|must.?haves?)",
    )
    bonus_headers = re.compile(
        r"(?i)(good.?to.?have|bonus(es)?|nice.?to.?have)",
    )

    lines = criteria.splitlines()
    current_bucket: list[str] | None = None
    criticals: list[str] = []
    bonuses: list[str] = []
    buffer = ""

    def flush():
        nonlocal buffer
        text = buffer.strip()
        if text:
            if current_bucket == "critical":
                criticals.append(text)
            elif current_bucket == "bonus":
                bonuses.append(text)
        buffer = ""

    for line in lines:
        stripped = line.strip()
        if not stripped:
            flush()
            continue
        if critical_headers.match(stripped):
            flush()
            current_bucket = "critical"
            continue
        if bonus_headers.match(stripped):
            flush()
            current_bucket = "bonus"
            continue
        if current_bucket:
            buffer += (" " if buffer else "") + stripped

    flush()
    return criticals, bonuses


class WatchlistAIScreener:
    def __init__(self, model: str = "gemini-2.5-flash"):
        self.client = genai.Client(api_key=GEMINI_KEY)
        self.model = model
        self._grounding_tool = types.Tool(google_search=types.GoogleSearch())

    # Public API
    def run(self, enriched_watchlist: list[dict], fundamentals_map: dict) -> list[dict]:
        items = [i for i in enriched_watchlist if i.get("criteria")]
        logger.info("AI screening %d items with criteria", len(items))

        alerts = []
        for item in items:
            try:
                alert = self._screen(item, fundamentals_map.get(item["ticker"], {}))
                if alert:
                    alerts.append(alert)
            except Exception:
                logger.exception("Screening failed for %s", item.get("ticker"))

        self._persist(alerts)
        return alerts

    def merge_alerts(self, rows: list[dict]) -> list[dict]:
        stored = self.read()
        by_ticker = {a["ticker"]: a for a in stored.get("alerts", [])}

        for row in rows:
            alert = by_ticker.get(row["ticker"])
            if alert:
                row["ai_alert"] = {
                    "ready_to_buy": alert.get("ready_to_buy", False),
                    "critical_conditions": alert.get("critical_conditions", []),
                    "bonus_conditions": alert.get("bonus_conditions", []),
                    "screened_at": alert.get("screened_at"),
                    "search_queries": alert.get("search_queries", []),
                }
            else:
                row["ai_alert"] = None

        return rows

    @staticmethod
    def read() -> dict:
        if not ALERTS_PATH.exists():
            return {"generated_at": None, "alerts": []}
        try:
            return json.loads(ALERTS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {"generated_at": None, "alerts": []}

    #  Internals
    def _screen(self, item: dict, fundamentals: dict) -> Optional[dict]:
        ticker = item["ticker"]
        note = item.get("notes") or ""
        criteria = item["criteria"]
        price = item.get("current_price")

        criticals, bonuses = _parse_conditions(criteria)

        if not criticals and not bonuses:
            logger.warning("%s: no parseable conditions found in criteria", ticker)
            return None

        prompt = _EVALUATOR_PROMPT.format(
            ticker=ticker,
            note=note,
            critical_conditions="\n".join(f"- {c}" for c in criticals) or "(none)",
            bonus_conditions="\n".join(f"- {b}" for b in bonuses) or "(none)",
            price=price or "unknown",
            fundamentals=(
                json.dumps(fundamentals, default=str) if fundamentals else "{}"
            ),
            today=date.today().isoformat(),
        )

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=[self._grounding_tool],
                temperature=0.1,
            ),
        )

        result = self._extract_json(response.text)
        if not result:
            logger.error("%s: failed to extract JSON from grounded response", ticker)
            return None

        search_queries: list[str] = []
        try:
            meta = response.candidates[0].grounding_metadata
            if meta and meta.web_search_queries:
                search_queries = list(meta.web_search_queries)
        except Exception:
            pass

        logger.info("%s → grounded searches: %s", ticker, search_queries)

        return {
            "ticker": ticker,
            "screened_at": datetime.utcnow().isoformat() + "Z",
            "search_queries": search_queries,
            "ready_to_buy": result.get("ready_to_buy", False),
            "critical_conditions": result.get("critical_conditions", []),
            "bonus_conditions": result.get("bonus_conditions", []),
        }

    @staticmethod
    def _extract_json(text: str) -> Optional[dict]:
        """Extract JSON from a code-fenced or raw response."""
        import re

        # Try fenced block first: ```json ... ``` or ``` ... ```
        match = re.search(r"```(?:json)?\s*(\{.*?})\s*```", text, re.DOTALL)
        if match:
            raw = match.group(1)
        else:
            # Fall back to first { ... } in the text
            match = re.search(r"(\{.*})", text, re.DOTALL)
            raw = match.group(1) if match else text.strip()
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _persist(self, alerts: list[dict]) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "alerts": alerts,
        }
        ALERTS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info("Persisted %d alerts", len(alerts))
