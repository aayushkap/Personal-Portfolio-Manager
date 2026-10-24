"""The single entry point for Gemini generation and per-invocation metrics."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from time import perf_counter

from google import genai
from google.genai import types

from app.config import GEMINI_KEY
from app.core.logger import get_logger
from app.data.db import DB
from app.data.llm_usage import LLMUsageStore

logger = get_logger()

# USD per million tokens. Snapshot the applied rates with each invocation so
# later price changes do not rewrite history. Unknown model/rate => null cost.
TOKEN_PRICES = {
    # https://ai.google.dev/gemini-api/docs/pricing (checked 2026-10-19).
    "gemini-2.5-flash": {
        "input": 0.30,
        "output": 2.50,
        "cached": 0.03,
        "source": "Google standard paid text pricing, 2026-10-19",
    },
    # Introductory input/output rates supplied by the user on 2026-10-19.
    "gemini-3.7-flash": {
        "input": 0.75,
        "output": 3.75,
        "cached": None,
        "source": "User-supplied introductory pricing, 2026-10-19",
    },
}


class GeminiLLM:
    def __init__(self, *, client=None, usage_store: LLMUsageStore | None = None):
        self._client = client
        self.usage_store = (
            usage_store
            if usage_store is not None
            else LLMUsageStore(DB(read_only=False))
        )

    def generate_content(
        self,
        *,
        operation: str,
        model: str,
        contents,
        config: types.GenerateContentConfig,
        ticker: str | None = None,
    ):
        started_at = datetime.now(timezone.utc).isoformat()
        started = perf_counter()
        response = None
        error_type = None
        try:
            if self._client is None:
                self._client = genai.Client(api_key=GEMINI_KEY)
            response = self._client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
            return response
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            duration_ms = round((perf_counter() - started) * 1000, 3)
            try:
                usage = self._usage(response)
                pricing = TOKEN_PRICES.get(model)
                self.usage_store.record(
                    {
                        "started_at": started_at,
                        "operation": operation,
                        "model": model,
                        "ticker": ticker,
                        "status": "error" if error_type else "success",
                        "duration_ms": duration_ms,
                        **usage,
                        "estimated_token_cost_usd": self._cost(usage, pricing),
                        "pricing_json": json.dumps(pricing) if pricing else None,
                        # Do not persist provider error messages, prompts or output.
                        "error_type": error_type,
                    }
                )
            except Exception:
                # A metrics failure must neither discard a paid response nor
                # mask the original provider exception. Make the gap visible.
                logger.exception("Could not record LLM invocation for %s", operation)

    @staticmethod
    def _usage(response) -> dict:
        metadata = getattr(response, "usage_metadata", None)
        fields = {
            "prompt_tokens": "prompt_token_count",
            "output_tokens": "candidates_token_count",
            "thinking_tokens": "thoughts_token_count",
            "cached_tokens": "cached_content_token_count",
            "tool_prompt_tokens": "tool_use_prompt_token_count",
            "total_tokens": "total_token_count",
        }
        if metadata is None:
            return dict.fromkeys(fields)
        usage = {key: getattr(metadata, field, None) for key, field in fields.items()}
        for key in ("thinking_tokens", "cached_tokens", "tool_prompt_tokens"):
            usage[key] = usage[key] or 0
        # A blocked response may have no generated candidates.
        usage["output_tokens"] = usage["output_tokens"] or 0
        return usage

    @staticmethod
    def _cost(usage: dict, pricing: dict | None) -> float | None:
        if pricing is None or usage["prompt_tokens"] is None:
            return None
        cached = usage["cached_tokens"]
        if cached and pricing["cached"] is None:
            return None
        # Cached tokens are included in prompt_tokens. Thinking is billed as
        # output; tool-use prompts are additional input tokens.
        return round(
            (
                (max(usage["prompt_tokens"] - cached, 0) + usage["tool_prompt_tokens"])
                * pricing["input"]
                + cached * (pricing["cached"] or 0)
                + (usage["output_tokens"] + usage["thinking_tokens"])
                * pricing["output"]
            )
            / 1_000_000,
            12,
        )
