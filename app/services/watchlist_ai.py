from __future__ import annotations

import json
import re
from datetime import date, datetime
from typing import Optional

from google import genai
from app.core.logger import get_logger
from app.scraper.serp import fetch_news_snippets
from app.config import CACHE_DIR, GEMINI_KEY

logger = get_logger()

ALERTS_PATH = CACHE_DIR / "watchlist_alerts.json"


_QUERY_GEN_PROMPT = """\
You are a research assistant helping monitor investment criteria.

Ticker: {ticker}
Company note: {note}
Criteria to monitor: {criteria}

Generate 1 to 3 precise Google search queries (each 10 words max) that together \
would surface the most relevant recent news to evaluate whether each part of this \
criteria has been met. Use the company name, not just the ticker symbol.

Return a JSON array of strings only. Example:
["PPL Kentucky rate case 2026", "PPL free cash flow outlook"]
"""


_EVALUATOR_PROMPT = """\
You are monitoring a stock watchlist. The stock has NOT been purchased. \
Your only job is to assess whether the buy conditions have been met.

Ticker: {ticker}
Analyst note: {note}
Buy conditions: {criteria}

Current data:
- Price: {price}
- Fundamentals: {fundamentals}

Recent news (queries: {search_queries}):
{news_text}

Today: {today}

For each buy condition, state whether it is met or not. \
One short sentence per condition. No essays. No filler.
Set ready_to_buy to true only if ALL conditions are met.
"""


_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "ready_to_buy": {
            "type": "boolean",
        },
        "conditions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "condition": {"type": "string"},
                    "met": {"type": "boolean"},
                    "note": {"type": "string"},
                },
                "required": ["condition", "met", "note"],
            },
        },
    },
    "required": ["ready_to_buy", "conditions"],
}


class WatchlistAIScreener:
    def __init__(self, model: str = "gemini-2.5-flash"):
        self.client = genai.Client(api_key=GEMINI_KEY)
        self.model = model

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
                    "conditions": alert.get("conditions", []),
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

    def _screen(self, item: dict, fundamentals: dict) -> Optional[dict]:
        ticker = item["ticker"]
        note = item.get("notes") or ""
        criteria = item["criteria"]
        price = item.get("current_price")

        queries = self._generate_queries(ticker, note, criteria)
        logger.info("%s → queries: %s", ticker, queries)

        seen_titles: set[str] = set()
        all_news: list[dict] = []
        for query in queries:
            for article in fetch_news_snippets(query, num=6):
                if article["title"] not in seen_titles:
                    seen_titles.add(article["title"])
                    article["_query"] = query
                    all_news.append(article)

        logger.info("%s → %d news items fetched across %d queries", ticker, len(all_news), len(queries))

        news_text = (
            "\n".join(
                f"[{n['source']} | {n['date']} | q: {n['_query']}] {n['title']}: {n['snippet']}"
                for n in all_news
            )
            or "No recent news found."
        )

        prompt = _EVALUATOR_PROMPT.format(
            ticker=ticker,
            note=note,
            criteria=criteria,
            price=price or "unknown",
            fundamentals=json.dumps(fundamentals, default=str) if fundamentals else "{}",
            search_queries=", ".join(f'"{q}"' for q in queries),
            news_text=news_text,
            today=date.today().isoformat(),
        )

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "temperature": 0.2,
                "response_json_schema": _RESPONSE_SCHEMA,
            },
        )

        result = json.loads(response.text)
        return {
            "ticker": ticker,
            "screened_at": datetime.utcnow().isoformat() + "Z",
            "search_queries": queries,
            "ready_to_buy": result.get("ready_to_buy", False),
            "conditions": result.get("conditions", []),
        }

    def _generate_queries(self, ticker: str, note: str, criteria: str) -> list[str]:
        prompt = _QUERY_GEN_PROMPT.format(ticker=ticker, note=note, criteria=criteria)
        resp = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "temperature": 0.1,
            },
        )
        raw = resp.text.strip()
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        try:
            queries = json.loads(raw)
            if isinstance(queries, list) and all(isinstance(q, str) for q in queries):
                return queries[:3]
        except Exception:
            pass
        return [raw.strip().strip('"')]

    def _persist(self, alerts: list[dict]) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "alerts": alerts,
        }
        ALERTS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info("Persisted %d alerts", len(alerts))