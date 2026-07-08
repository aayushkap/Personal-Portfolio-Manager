from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from typing import Optional

from google import genai
from google.genai import types

from app.core.logger import get_logger
from app.config import CACHE_DIR, GEMINI_KEY

from app.hql import HQL

logger = get_logger()

NEWS_PATH = CACHE_DIR / "holdings_news.json"

MIN_CHECK_INTERVAL_DAYS = 1
MAX_CHECK_INTERVAL_DAYS = 7  # hard weekly ceiling
NEW_DEVELOPMENT_WINDOW_DAYS = 2
BOOTSTRAP_LOOKBACK_DAYS = 14  # first-ever run window when no news exists yet

_NEWS_PROMPT = """\
You are a financial news analyst monitoring a stock the investor already HOLDS.
Your job is to find NEW, MATERIAL news about this company that could reasonably
affect its stock price or an investor's decision to keep holding it.

Ticker: {ticker}
Company/exchange context: {info}

You have access to Google Search — use it to find current, specific, dated news.
Do not rely on memory. Do not invent dates.

CRITICAL RULE: Only report news dated STRICTLY AFTER {since_date}.
Do not repeat, rephrase, or re-report anything at or before that date, even if
it still seems relevant. If genuinely nothing material happened after that date,
return an empty news list — do not pad with old or filler items.

Focus only on things that matter to an investor holding this stock:
- Earnings results or updated guidance
- Analyst rating or price target changes (only if from a notable firm and a
  meaningful magnitude)
- M&A, partnerships, major contracts
- Regulatory action, lawsuits, investigations
- Leadership changes (CEO/CFO/board)
- Product launches or major operational changes
- Significant competitor moves that materially affect this company's outlook
- Dividend changes, buybacks, capital structure changes
- Macro/sector news ONLY if it specifically and materially impacts this ticker

Ignore routine noise: minor price commentary, generic "stock moved X%" articles
with no underlying catalyst, syndicated boilerplate, or repeated wire summaries.

Today's date: {today}

For next_check_date: pick the earliest date you'd expect new material information
(e.g. next earnings date, a known upcoming catalyst). If nothing specific is
known, default to 7 days from today. Never go below {min_date} or above {max_date}.

Return your response as a JSON object wrapped in a ```json code block. Use this
exact structure:
```json
{{
  "news": [
    {{
      "date": "YYYY-MM-DD",
      "headline": "...",
      "summary": "...",
      "category": "earnings|guidance|analyst_rating|ma|regulatory|leadership|product|competitor|dividend|macro|other",
      "sentiment": "positive|negative|neutral",
      "source": "..."
    }}
  ],
  "next_check_date": "{default_next}"
}}
```
"""


class HoldingsNewsAgent:
    def __init__(self, model: str = "gemini-2.5-flash"):
        self.client = genai.Client(api_key=GEMINI_KEY)
        self.model = model
        self._grounding_tool = types.Tool(google_search=types.GoogleSearch())

    # Public API
    def run(self) -> dict:
        """
        holdings: list of dicts with at least {"ticker": ..., "name": ..., "sector": ...}
        Only processes tickers whose next_check_date has arrived (or have never
        been checked). Returns the updated store.
        """
        store = self.read()
        today = date.today()

        hql = HQL()
        holdings_df = hql.portfolio().holdings()
        holdings = [
            {
                "ticker": row["ticker"],
                "name": hql.ticker(row["ticker"]).info().get("symbol"),
                "sector": hql.ticker(row["ticker"]).info().get("sector"),
                "exchange": hql.ticker(row["ticker"]).info().get("exchange"),
            }
            for _, row in holdings_df.iterrows()
            if row["shares"] > 0
        ]

        for h in holdings:
            ticker = h["ticker"]
            entry = store["tickers"].get(ticker)

            due = True
            if entry and entry.get("next_check_date"):
                try:
                    due = date.fromisoformat(entry["next_check_date"]) <= today
                except (ValueError, TypeError):
                    due = True

            if not due:
                continue

            try:
                self._check_ticker(store, ticker, h)
            except Exception:
                logger.exception("News check failed for %s", ticker)

        store["generated_at"] = datetime.utcnow().isoformat() + "Z"
        self._persist(store)
        return store

    def merge_news(self, rows: list[dict]) -> list[dict]:
        """Attach news + has_new_development to holdings list output."""
        store = self.read()
        today = date.today()

        for row in rows:
            entry = store["tickers"].get(row["ticker"])
            if not entry:
                row["news"] = []
                row["has_new_development"] = False
                continue

            news = entry.get("news", [])
            has_new = any(
                self._is_recent(item.get("date"), today, NEW_DEVELOPMENT_WINDOW_DAYS)
                for item in news
            )
            row["news"] = news
            row["has_new_development"] = has_new

        return rows

    @staticmethod
    def read() -> dict:
        if not NEWS_PATH.exists():
            return {"generated_at": None, "tickers": {}}
        try:
            data = json.loads(NEWS_PATH.read_text(encoding="utf-8"))
            data.setdefault("tickers", {})
            return data
        except Exception:
            return {"generated_at": None, "tickers": {}}

    # Internals
    def _check_ticker(self, store: dict, ticker: str, holding: dict) -> None:
        today = date.today()
        entry = store["tickers"].setdefault(
            ticker, {"news": [], "last_checked": None, "next_check_date": None}
        )

        existing_news = entry.get("news", [])
        since_date = self._latest_news_date(existing_news)
        if since_date is None:
            since_date = today - timedelta(days=BOOTSTRAP_LOOKBACK_DAYS)

        min_date = (today + timedelta(days=MIN_CHECK_INTERVAL_DAYS)).isoformat()
        max_date = (today + timedelta(days=MAX_CHECK_INTERVAL_DAYS)).isoformat()
        default_next = (today + timedelta(days=MAX_CHECK_INTERVAL_DAYS)).isoformat()

        info = {
            "name": holding.get("name"),
            "sector": holding.get("sector"),
            "exchange": holding.get("exchange"),
        }

        prompt = _NEWS_PROMPT.format(
            ticker=ticker,
            info=json.dumps(info, default=str),
            since_date=since_date.isoformat(),
            today=today.isoformat(),
            min_date=min_date,
            max_date=max_date,
            default_next=default_next,
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
            logger.error("%s: failed to extract JSON from news response", ticker)
            entry["next_check_date"] = default_next
            entry["last_checked"] = datetime.utcnow().isoformat() + "Z"
            return

        new_items = self._normalize_items(result.get("news", []), since_date, today)
        deduped = self._dedupe(existing_news, new_items)

        entry["news"] = sorted(
            existing_news + deduped, key=lambda x: x["date"], reverse=True
        )

        raw_next = result.get("next_check_date", default_next)
        entry["next_check_date"] = self._clamp_next_check(raw_next, today)
        entry["last_checked"] = datetime.utcnow().isoformat() + "Z"

        search_queries: list[str] = []
        try:
            meta = response.candidates.grounding_metadata
            if meta and meta.web_search_queries:
                search_queries = list(meta.web_search_queries)
        except Exception:
            pass

        logger.info(
            "%s: found %d new item(s), next_check=%s, searches=%s",
            ticker,
            len(deduped),
            entry["next_check_date"],
            search_queries,
        )

    @staticmethod
    def _latest_news_date(news: list[dict]) -> Optional[date]:
        dates = []
        for item in news:
            try:
                dates.append(date.fromisoformat(item["date"]))
            except (ValueError, TypeError, KeyError):
                continue
        return max(dates) if dates else None

    @staticmethod
    def _normalize_items(
        raw_items: list[dict], since_date: date, today: date
    ) -> list[dict]:
        out = []
        for item in raw_items:
            try:
                item_date = date.fromisoformat(item.get("date", ""))
            except (ValueError, TypeError):
                continue

            # Hard enforcement: never trust the LLM's date filtering alone
            if item_date <= since_date or item_date > today:
                continue

            headline = (item.get("headline") or "").strip()
            if not headline:
                continue

            out.append(
                {
                    "date": item_date.isoformat(),
                    "headline": headline,
                    "summary": (item.get("summary") or "").strip(),
                    "category": item.get("category") or "other",
                    "sentiment": item.get("sentiment") or "neutral",
                    "source": item.get("source") or "",
                    "is_new_development": HoldingsNewsAgent._is_recent(
                        item_date.isoformat(), today, NEW_DEVELOPMENT_WINDOW_DAYS
                    ),
                }
            )
        return out

    @staticmethod
    def _dedupe(existing: list[dict], new_items: list[dict]) -> list[dict]:
        def _norm(h: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", h.lower())

        existing_keys = {_norm(item["headline"]) for item in existing}
        deduped = []
        for item in new_items:
            key = _norm(item["headline"])
            if key in existing_keys:
                continue
            existing_keys.add(key)
            deduped.append(item)
        return deduped

    @staticmethod
    def _clamp_next_check(raw: str, today: date) -> str:
        try:
            parsed = date.fromisoformat(raw)
        except (ValueError, TypeError):
            parsed = today + timedelta(days=MAX_CHECK_INTERVAL_DAYS)
        parsed = max(parsed, today + timedelta(days=MIN_CHECK_INTERVAL_DAYS))
        parsed = min(parsed, today + timedelta(days=MAX_CHECK_INTERVAL_DAYS))
        return parsed.isoformat()

    @staticmethod
    def _is_recent(date_str: Optional[str], today: date, window_days: int) -> bool:
        if not date_str:
            return False
        try:
            d = date.fromisoformat(date_str)
        except (ValueError, TypeError):
            return False
        return (today - d).days <= window_days and d <= today

    @staticmethod
    def _extract_json(text: str) -> Optional[dict]:
        match = re.search(r"```(?:json)?\s*(\{.*?})\s*```", text, re.DOTALL)
        raw = (
            match.group(1)
            if match
            else (
                re.search(r"(\{.*})", text, re.DOTALL).group(1)
                if re.search(r"(\{.*})", text, re.DOTALL)
                else text.strip()
            )
        )
        try:
            return json.loads(raw)
        except Exception:
            return None

    def _persist(self, store: dict) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        NEWS_PATH.write_text(json.dumps(store, indent=2), encoding="utf-8")
        logger.info("Persisted news for %d ticker(s)", len(store["tickers"]))
