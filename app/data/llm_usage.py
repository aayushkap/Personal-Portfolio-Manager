"""Persist LLM usage in the worker; read shared metrics from API processes."""

import json

from app.data.db import DB


class LLMUsageStore:
    def __init__(self, db: DB | None = None):
        self.db = db if db is not None else DB()

    def record(self, invocation: dict) -> None:
        columns = ", ".join(invocation)
        placeholders = ", ".join("?" for _ in invocation)
        with self.db.connection(write=True) as conn:
            conn.execute(
                f"INSERT INTO llm_invocations ({columns}) VALUES ({placeholders})",
                tuple(invocation.values()),
            )

    def metrics(self, *, limit: int = 50, offset: int = 0) -> dict:
        # Aggregate the entire history independently of the recent-call page.
        aggregate = """
            COUNT(*) AS invocations,
            COALESCE(SUM(status = 'error'), 0) AS failed_invocations,
            COALESCE(SUM(estimated_token_cost_usd IS NULL), 0) AS unpriced_invocations,
            COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
            COALESCE(SUM(output_tokens), 0) AS output_tokens,
            COALESCE(SUM(thinking_tokens), 0) AS thinking_tokens,
            COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
            COALESCE(SUM(tool_prompt_tokens), 0) AS tool_prompt_tokens,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            SUM(estimated_token_cost_usd) AS estimated_token_cost_usd
        """
        with self.db.connection() as conn:
            totals = dict(
                conn.execute(f"SELECT {aggregate} FROM llm_invocations").fetchone()
            )
            groups = [
                dict(row)
                for row in conn.execute(
                    f"SELECT operation, model, {aggregate} FROM llm_invocations "
                    "GROUP BY operation, model ORDER BY operation, model"
                )
            ]
            recent = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM llm_invocations ORDER BY id DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                )
            ]
        for invocation in recent:
            invocation["pricing"] = json.loads(invocation.pop("pricing_json") or "null")
        return {
            "available": True,
            "currency": "USD",
            "period": "all_time",
            "cost_basis": "estimated_token_cost; excludes grounding, cache storage, "
            "free-tier allowances, discounts and taxes",
            "totals": totals,
            "by_operation_model": groups,
            "recent": recent,
            "limit": limit,
            "offset": offset,
        }
