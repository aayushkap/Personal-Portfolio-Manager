from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from google.genai import types

from app.api.deps import get_system_module
from app.api.system import router
from app.data.db import DB
from app.data.llm_usage import LLMUsageStore
from app.services.holdings_news import HoldingsNewsAgent
from app.services.llm import GeminiLLM
from app.services.quote import QuoteStore
from app.services.system import SystemModule
from app.services.watchlist_ai import WatchlistAIScreener


class LLMUsageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "portfolio.db"
        DB.bootstrap(self.path)
        self.store = LLMUsageStore(DB(self.path, read_only=False))
        self.client = Mock()
        self.llm = GeminiLLM(client=self.client, usage_store=self.store)
        self.config = types.GenerateContentConfig(temperature=0.1)

    def response(self, **usage):
        # Use the real SDK model to catch SDK versions that drop usage fields.
        return types.GenerateContentResponse(
            usage_metadata=types.GenerateContentResponseUsageMetadata(**usage)
        )

    def invoke(self, model="gemini-2.5-flash"):
        return self.llm.generate_content(
            operation="test",
            ticker="NYSE:TEST",
            model=model,
            contents="private prompt",
            config=self.config,
        )

    def test_cost_includes_thinking_tools_and_discounted_cached_tokens(self):
        response = self.response(
            prompt_token_count=1000,
            candidates_token_count=100,
            thoughts_token_count=200,
            cached_content_token_count=400,
            tool_use_prompt_token_count=50,
            total_token_count=1350,
        )
        self.client.models.generate_content.return_value = response
        self.assertIs(self.invoke(), response)
        self.client.models.generate_content.assert_called_once_with(
            model="gemini-2.5-flash",
            contents="private prompt",
            config=self.config,
        )
        # (600 + 50)*0.30 + 400*0.03 + (100 + 200)*2.50, per million.
        row = self.store.metrics()["recent"][0]
        self.assertAlmostEqual(row["estimated_token_cost_usd"], 0.000957)
        self.assertEqual(row["thinking_tokens"], 200)
        self.assertEqual(row["tool_prompt_tokens"], 50)
        self.assertEqual(row["status"], "success")
        self.assertGreaterEqual(row["duration_ms"], 0)
        self.assertEqual(row["ticker"], "NYSE:TEST")
        self.assertNotIn("private prompt", json.dumps(row))
        self.assertEqual(row["pricing"]["input"], 0.30)

    def test_user_supplied_introductory_quote_pricing(self):
        self.client.models.generate_content.return_value = self.response(
            prompt_token_count=1000,
            candidates_token_count=100,
            thoughts_token_count=200,
            total_token_count=1300,
        )
        self.invoke(model="gemini-3.7-flash")
        row = self.store.metrics()["recent"][0]
        self.assertAlmostEqual(row["estimated_token_cost_usd"], 0.001875)
        self.assertIn("User-supplied", row["pricing"]["source"])

    def test_provider_failure_is_recorded_and_original_exception_propagates(self):
        error = RuntimeError("private provider detail")
        self.client.models.generate_content.side_effect = error
        with self.assertRaises(RuntimeError) as caught:
            self.invoke()
        self.assertIs(caught.exception, error)
        row = self.store.metrics()["recent"][0]
        self.assertEqual(row["status"], "error")
        self.assertEqual(row["error_type"], "RuntimeError")
        self.assertIsNone(row["estimated_token_cost_usd"])
        self.assertIsNone(row["total_tokens"])
        self.assertNotIn("private provider detail", json.dumps(row))

    def test_unknown_pricing_or_missing_usage_remains_null(self):
        responses = [
            ("unknown-model", self.response(prompt_token_count=10)),
            ("gemini-2.5-flash", types.GenerateContentResponse()),
            (
                "gemini-3.7-flash",
                self.response(
                    prompt_token_count=10,
                    cached_content_token_count=5,
                ),
            ),
        ]
        for model, response in responses:
            self.client.models.generate_content.return_value = response
            self.invoke(model)
        metrics = self.store.metrics()
        self.assertEqual(metrics["totals"]["unpriced_invocations"], 3)
        self.assertIsNone(metrics["totals"]["estimated_token_cost_usd"])

    def test_blocked_response_still_records_prompt_usage_and_cost(self):
        self.client.models.generate_content.return_value = self.response(
            prompt_token_count=1000,
            total_token_count=1000,
        )
        self.invoke()
        row = self.store.metrics()["recent"][0]
        self.assertEqual(row["output_tokens"], 0)
        self.assertAlmostEqual(row["estimated_token_cost_usd"], 0.0003)

    def test_database_reads_persist_across_bootstrap_and_pagination(self):
        self.client.models.generate_content.return_value = self.response(
            prompt_token_count=1000,
            candidates_token_count=100,
            total_token_count=1100,
        )
        self.invoke()
        self.invoke()
        self.client.models.generate_content.side_effect = RuntimeError()
        with self.assertRaises(RuntimeError):
            self.invoke()
        DB.bootstrap(self.path)
        # Independent API connection, read-only and without a provider client.
        metrics = LLMUsageStore(DB(self.path)).metrics(limit=1, offset=1)
        self.assertEqual(metrics["totals"]["invocations"], 3)
        self.assertEqual(metrics["totals"]["failed_invocations"], 1)
        self.assertEqual(metrics["totals"]["unpriced_invocations"], 1)
        self.assertEqual(metrics["totals"]["total_tokens"], 2200)
        self.assertAlmostEqual(metrics["totals"]["estimated_token_cost_usd"], 0.0011)
        self.assertEqual(metrics["by_operation_model"][0]["invocations"], 3)
        self.assertEqual(len(metrics["recent"]), 1)
        self.assertEqual(metrics["recent"][0]["id"], 2)

    def test_recording_failure_preserves_response_and_provider_error(self):
        self.llm.usage_store = Mock()
        self.llm.usage_store.record.side_effect = OSError("unavailable")
        response = self.response(prompt_token_count=10)
        self.client.models.generate_content.return_value = response
        with self.assertLogs("app", level="ERROR"):
            self.assertIs(self.invoke(), response)
        self.client.models.generate_content.side_effect = ValueError("provider")
        with (
            self.assertLogs("app", level="ERROR"),
            self.assertRaisesRegex(ValueError, "provider"),
        ):
            self.invoke()

    def test_all_three_services_record_through_the_shared_wrapper(self):
        quote = QuoteStore()
        quote.llm = self.llm
        self.client.models.generate_content.return_value = SimpleNamespace(
            text='{"quote": "test", "author": "test"}',
            usage_metadata=self.response(prompt_token_count=10).usage_metadata,
        )
        with (
            patch.object(quote, "_load_history", return_value=[]),
            patch.object(quote, "_save_history"),
            patch("app.services.quote.atomic_write_json"),
        ):
            quote.write()

        screener = WatchlistAIScreener()
        screener.llm = self.llm
        self.client.models.generate_content.return_value.text = (
            '{"ready_to_buy": false}'
        )
        screener._screen({"ticker": "NYSE:TEST", "criteria": ["test"]}, {})

        news = HoldingsNewsAgent()
        news.llm = self.llm
        self.client.models.generate_content.return_value.text = '{"news": []}'
        news._check_ticker({"tickers": {}}, "NYSE:TEST", {})
        metrics = self.store.metrics()
        self.assertEqual(metrics["totals"]["invocations"], 3)
        self.assertEqual(
            {row["operation"] for row in metrics["recent"]},
            {"quote", "watchlist_screening", "holdings_news"},
        )

    def test_system_api_exposes_metrics_and_validates_pagination(self):
        self.client.models.generate_content.return_value = self.response(
            prompt_token_count=10
        )
        self.invoke()
        with patch("app.services.system.psutil.process_iter", return_value=[]):
            module = SystemModule(db=DB(self.path))
        app = FastAPI()
        app.include_router(router)
        app.dependency_overrides[get_system_module] = lambda: module
        with TestClient(app) as client:
            metrics = client.post("/system/llm?limit=1&offset=0")
            self.assertEqual(metrics.status_code, 200)
            self.assertEqual(metrics.json()["totals"]["invocations"], 1)
            self.assertEqual(client.post("/system/llm?limit=501").status_code, 422)
            self.assertEqual(client.post("/system/llm?offset=-1").status_code, 422)
            # Keep the HTTP/health integration independent of host sensors.
            methods = (
                "_get_host_info",
                "_get_cpu",
                "_get_memory",
                "_get_swap",
                "_get_disks",
                "_get_disk_io",
                "_get_network",
                "_get_power",
                "_get_temperatures",
                "_get_top_processes",
            )
            with ExitStack() as stack:
                stack.enter_context(
                    patch("app.services.system.psutil.boot_time", return_value=0)
                )
                for method in methods:
                    stack.enter_context(patch.object(module, method, return_value={}))
                health = client.post("/system/health")
            self.assertEqual(health.status_code, 200)
            self.assertEqual(health.json()["llm"]["totals"]["invocations"], 1)

    def test_missing_schema_leaves_health_metrics_explicitly_unavailable(self):
        with self.store.db.connection(write=True) as conn:
            conn.execute("DROP TABLE llm_invocations")
        with patch("app.services.system.psutil.process_iter", return_value=[]):
            module = SystemModule(db=DB(self.path))
        with self.assertLogs("app", level="ERROR"):
            self.assertFalse(module.get_llm_metrics()["available"])


if __name__ == "__main__":
    unittest.main()
