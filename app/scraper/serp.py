from __future__ import annotations

import os
import httpx
from app.core.logger import get_logger

logger = get_logger()

_BASE = "https://serpapi.com/search.json"


def fetch_news_snippets(query: str, num: int = 8) -> list[dict]:
    api_key = os.getenv("SERP_API_KEY")
    if not api_key:
        logger.warning("SERP_API_KEY not set — skipping news fetch")
        return []
    try:
        resp = httpx.get(
            _BASE,
            params={
                "engine": "google_news",
                "q": query,
                "num": num,
                "api_key": api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return [
            {
                "title": r.get("title", ""),
                "snippet": r.get("snippet", ""),
                "date": r.get("date", ""),
                "source": r.get("source", {}).get("name", ""),
            }
            for r in resp.json().get("news_results", [])
        ]
    except Exception as exc:
        logger.error("SerpAPI failed for '%s': %s", query, exc)
        return []
