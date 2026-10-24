import copy
import json
import tempfile
import unittest
from pathlib import Path

from app.data.cache import Cache
from app.data.merge import MergeReport, merge_dividends, merge_table, merge_ticker
from app.data.schemas import DividendsData, TabularData
from app.hql.queries import _tabular_rows_to_df

LABEL = "Fiscal Year"


def table(moving, years, rows, **extra):
    """rows: {label: [moving_value, FY values newest first...]}"""
    headers = [LABEL, moving] + [f"FY {y}" for y in years]
    return {
        "symbol": "ABC",
        "headers": headers,
        "rows": [
            dict(zip(headers, [label, *values])) for label, values in rows.items()
        ],
        **extra,
    }


def divs(*rows, headers=("Ex-Dividend Date", "Cash Amount", "Record Date", "Pay Date")):
    return {"headers": list(headers), "rows": [dict(zip(headers, r)) for r in rows]}


def row(section, label):
    return next(r for r in section["rows"] if r[LABEL] == label)


class TableMergeTests(unittest.TestCase):
    def merge(self, old, new):
        report = MergeReport()
        return merge_table(old, new, report, "financials"), report

    def test_year_roll_retains_oldest_year_and_replaces_moving_column(self):
        old = table(
            "TTM",
            [2025, 2024, 2023, 2022, 2021],
            {"Revenue": ["60", "50", "40", "30", "20", "10"]},
        )
        new = table(
            "TTM",
            [2026, 2025, 2024, 2023, 2022],
            {"Revenue": ["75", "70", "50", "40", "30", "20"]},
        )
        merged, _ = self.merge(old, new)
        self.assertEqual(
            merged["headers"],
            [
                LABEL,
                "TTM",
                "FY 2026",
                "FY 2025",
                "FY 2024",
                "FY 2023",
                "FY 2022",
                "FY 2021",
            ],
        )
        self.assertEqual(row(merged, "Revenue")["TTM"], "75")
        self.assertEqual(row(merged, "Revenue")["FY 2021"], "10")
        self.assertEqual(row(merged, "Revenue")["FY 2026"], "70")

    def test_history_accumulates_across_repeated_rolls(self):
        doc = table("TTM", [2023, 2022, 2021], {"Revenue": ["4", "3", "2", "1"]})
        for year in (2024, 2025, 2026):
            years = [year, year - 1, year - 2]
            doc, _ = self.merge(
                doc,
                table(
                    "TTM", years, {"Revenue": ["9"] + [str(y - 2020) for y in years]}
                ),
            )
        self.assertEqual(
            [h for h in doc["headers"] if h.startswith("FY")][-1], "FY 2021"
        )
        self.assertEqual(len(doc["headers"]), 2 + 6)
        self.assertEqual(row(doc, "Revenue")["FY 2021"], "1")

    def test_restated_value_inside_window_is_updated(self):
        old = table("TTM", [2025, 2024], {"Revenue": ["9", "50", "40"]})
        new = table("TTM", [2025, 2024], {"Revenue": ["9", "51", "40"]})
        merged, report = self.merge(old, new)
        self.assertEqual(row(merged, "Revenue")["FY 2025"], "51")
        self.assertIn("financials: 1 cached value(s) restated by source", report.notes)

    def test_placeholder_never_erases_fiscal_year_but_moving_column_takes_latest(self):
        old = table("TTM", [2025, 2024], {"Revenue": ["9", "50", "40"]})
        new = table("TTM", [2025, 2024], {"Revenue": ["-", "Upgrade", "40"]})
        merged, _ = self.merge(old, new)
        self.assertEqual(row(merged, "Revenue")["FY 2025"], "50")
        self.assertEqual(row(merged, "Revenue")["TTM"], "-")

    def test_year_dropped_inside_source_window_is_removed(self):
        old = table("TTM", [2025, 2024, 2023], {"Revenue": ["9", "3", "2", "1"]})
        new = {**old, "headers": [LABEL, "TTM", "FY 2025", "FY 2023"]}
        new["rows"] = [
            {k: v for k, v in r.items() if k != "FY 2024"} for r in old["rows"]
        ]
        merged, report = self.merge(old, new)
        self.assertNotIn("FY 2024", merged["headers"])
        self.assertNotIn("FY 2024", merged["rows"][0])
        self.assertTrue(any("no longer lists ['FY 2024']" in n for n in report.notes))

    def test_stale_moving_column_is_not_kept(self):
        old = table("TTM", [2025, 2024], {"Revenue": ["9", "3", "2"]})
        new = table("TTM", [2025, 2024], {"Revenue": ["9", "3", "2"]})
        new["headers"].remove("TTM")
        del new["rows"][0]["TTM"]
        merged, _ = self.merge(old, new)
        self.assertNotIn("TTM", merged["headers"])
        self.assertNotIn("TTM", merged["rows"][0])

    def test_dropped_line_item_survives_only_for_retained_years(self):
        old = table(
            "TTM",
            [2025, 2024, 2023],
            {
                "Revenue": ["9", "3", "2", "1"],
                "Old Item": ["8", "7", "6", "5"],
                "Tail": ["1", "1", "1", "1"],
            },
        )
        new = table(
            "TTM",
            [2026, 2025, 2024],
            {
                "Revenue": ["9", "4", "3", "2"],
                "New Item": ["1", "1", "1", "1"],
                "Tail": ["1", "1", "1", "1"],
            },
        )
        merged, _ = self.merge(old, new)
        self.assertEqual(
            [r[LABEL] for r in merged["rows"]],
            ["Revenue", "Old Item", "New Item", "Tail"],
        )
        self.assertEqual(
            row(merged, "Old Item"),
            {
                LABEL: "Old Item",
                "TTM": "-",
                "FY 2026": "-",
                "FY 2025": "-",
                "FY 2024": "-",
                "FY 2023": "5",
            },
        )
        self.assertEqual(row(merged, "New Item")["FY 2023"], "-")

    def test_dropped_line_item_without_retained_values_is_removed(self):
        old = table(
            "TTM",
            [2025, 2024],
            {"Revenue": ["9", "3", "2"], "Renamed": ["1", "1", "1"]},
        )
        new = table("TTM", [2025, 2024], {"Revenue": ["9", "3", "2"]})
        merged, _ = self.merge(old, new)
        self.assertEqual([r[LABEL] for r in merged["rows"]], ["Revenue"])

    def test_split_rebases_retained_per_share_values_only(self):
        old = table(
            "TTM",
            [2025, 2024, 2023, 2022, 2021],
            {
                "Revenue": ["9", "50", "40", "30", "20", "10"],
                "EPS (Diluted)": ["8.72", "7.46", "6.08", "6.13", "6.11", "5.61"],
            },
        )
        new = table(
            "TTM",
            [2026, 2025, 2024, 2023, 2022],
            {
                "Revenue": ["9", "60", "50", "40", "30", "20"],
                "EPS (Diluted)": ["0.44", "0.41", "0.37", "0.30", "0.31", "0.31"],
            },
        )
        merged, report = self.merge(old, new)
        self.assertEqual(row(merged, "EPS (Diluted)")["FY 2021"], "0.2807")
        self.assertEqual(row(merged, "Revenue")["FY 2021"], "10")
        self.assertIn("financials: rescaled retained years on 1 row(s)", report.notes)

    def test_single_changed_overlap_is_a_restatement_not_a_rebase(self):
        old = table("TTM", [2025, 2024, 2023], {"EPS": ["9", "2.00", "1.00", "0.50"]})
        new = table("TTM", [2026, 2025, 2024], {"EPS": ["9", "3.00", "2.20", "1.00"]})
        merged, _ = self.merge(old, new)
        self.assertEqual(row(merged, "EPS")["FY 2023"], "0.50")

    def test_inconsistent_wholesale_rebase_discards_retained_history(self):
        labels = {f"Item {i}": ["1", "100", "100", "100", "100"] for i in range(4)}
        old = table("TTM", [2025, 2024, 2023, 2022], labels)
        rebased = {k: ["1", "9", "27", "31", "37"] for k in labels}
        new = table("TTM", [2026, 2025, 2024, 2023], rebased)
        merged, report = self.merge(old, new)
        self.assertNotIn("FY 2022", merged["headers"])
        self.assertIs(report.superseded["financials"], old)

    def test_new_table_without_years_keeps_all_cached_years(self):
        old = table("TTM", [2025, 2024], {"Revenue": ["9", "3", "2"]})
        new = {"headers": [LABEL, "TTM"], "rows": [{LABEL: "Revenue", "TTM": "10"}]}
        merged, _ = self.merge(old, new)
        self.assertEqual(
            merged["rows"],
            [{LABEL: "Revenue", "TTM": "10", "FY 2025": "3", "FY 2024": "2"}],
        )

    def test_empty_new_table_keeps_cached_table(self):
        old = table("TTM", [2025], {"Revenue": ["9", "3"]})
        merged, _ = self.merge(old, {"headers": [], "rows": []})
        self.assertIs(merged, old)

    def test_changed_label_column_replaces_instead_of_merging(self):
        old = table("TTM", [2025, 2024], {"Revenue": ["9", "3", "2"]})
        new = {
            "headers": ["Fiscal Quarter", "Q1 2026"],
            "rows": [{"Fiscal Quarter": "Revenue", "Q1 2026": "1"}],
        }
        merged, _ = self.merge(old, new)
        self.assertIs(merged, new)

    def test_repeated_labels_merge_by_occurrence(self):
        old = {
            "headers": [LABEL, "FY 2025", "FY 2024"],
            "rows": [
                {LABEL: "Growth", "FY 2025": "1", "FY 2024": "2"},
                {LABEL: "Growth", "FY 2025": "3", "FY 2024": "4"},
            ],
        }
        new = {
            "headers": [LABEL, "FY 2026", "FY 2025"],
            "rows": [
                {LABEL: "Growth", "FY 2026": "0", "FY 2025": "1"},
                {LABEL: "Growth", "FY 2026": "5", "FY 2025": "3"},
            ],
        }
        merged, _ = self.merge(old, new)
        self.assertEqual([r["FY 2024"] for r in merged["rows"]], ["2", "4"])

    def test_ratio_return_metrics_are_rebuilt_from_merged_rows(self):
        old = table(
            "Current",
            [2025, 2024],
            {"PE Ratio": ["1", "2", "3"], "Return on Equity (ROE)": ["1%", "2%", "3%"]},
        )
        old["return_metrics_rows"] = [old["rows"][1]]
        new = table(
            "Current",
            [2026, 2025],
            {"PE Ratio": ["1", "1", "2"], "Return on Equity (ROE)": ["4%", "5%", "2%"]},
        )
        new["return_metrics_rows"] = [new["rows"][1]]
        merged = merge_table(old, new, MergeReport(), "ratios")
        self.assertEqual(
            merged["return_metrics_rows"], [row(merged, "Return on Equity (ROE)")]
        )
        self.assertEqual(merged["return_metrics_rows"][0]["FY 2024"], "3%")

    def test_output_keeps_consumer_schema(self):
        old = table("TTM", [2025, 2024], {"Revenue": ["9", "3", "2"]})
        merged, _ = self.merge(
            old, table("TTM", [2026, 2025], {"Revenue": ["9", "4", "3"]})
        )
        TabularData.model_validate(merged)
        df = _tabular_rows_to_df(merged["rows"], merged["headers"])
        self.assertEqual(list(df.columns), ["TTM", "FY 2026", "FY 2025", "FY 2024"])
        self.assertEqual(df.loc["Revenue", "FY 2024"], 2.0)
        self.assertTrue(all(set(r) == set(merged["headers"]) for r in merged["rows"]))


class DividendMergeTests(unittest.TestCase):
    def merge(self, old, new):
        report = MergeReport()
        return merge_dividends(old, new, report), report

    def test_event_that_scrolled_off_is_retained_in_date_order(self):
        old = divs(
            ("2025-06-01", "$1.00", "2025-06-02", "2025-06-10"),
            ("2021-06-01", "$0.50", None, None),
        )
        new = divs(
            ("2026-06-01", "$1.10", None, None),
            ("2025-06-01", "$1.00", "2025-06-02", "2025-06-10"),
        )
        merged, _ = self.merge(old, new)
        self.assertEqual(
            [r["Ex-Dividend Date"] for r in merged["rows"]],
            ["2026-06-01", "2025-06-01", "2021-06-01"],
        )
        DividendsData.model_validate(merged)

    def test_updated_event_takes_source_values(self):
        old = divs(
            ("2026-06-01", "$1.10", None, None), ("2025-06-01", "$1.00", None, None)
        )
        new = divs(
            ("2026-06-01", "$1.15", "2026-06-02", "2026-06-20"),
            ("2025-06-01", "$1.00", None, None),
        )
        merged, report = self.merge(old, new)
        self.assertEqual(
            merged["rows"][0],
            {
                "Ex-Dividend Date": "2026-06-01",
                "Cash Amount": "$1.15",
                "Record Date": "2026-06-02",
                "Pay Date": "2026-06-20",
            },
        )
        self.assertIn("dividends: 1 cached event(s) updated by source", report.notes)

    def test_placeholder_does_not_erase_known_pay_date(self):
        old = divs(("2026-06-01", "$1.10", None, "2026-06-20"))
        new = divs(("2026-06-01", "$1.10", None, None))
        merged, _ = self.merge(old, new)
        self.assertEqual(merged["rows"][0]["Pay Date"], "2026-06-20")

    def test_event_removed_inside_source_window_is_dropped(self):
        old = divs(
            ("2026-06-01", "$1", None, None),
            ("2026-03-01", "$9", None, None),
            ("2025-12-01", "$1", None, None),
        )
        new = divs(("2026-06-01", "$1", None, None), ("2025-12-01", "$1", None, None))
        merged, _ = self.merge(old, new)
        self.assertNotIn("2026-03-01", [r["Ex-Dividend Date"] for r in merged["rows"]])

    def test_multiple_events_on_one_ex_date_are_replaced_as_a_group(self):
        old = divs(
            ("2026-06-01", "$1", None, None),
            ("2026-06-01", "$5", None, None),
            ("2020-01-01", "$1", None, None),
        )
        new = divs(("2026-06-01", "$1", None, None), ("2026-06-01", "$5", None, None))
        merged, _ = self.merge(old, new)
        self.assertEqual([r["Cash Amount"] for r in merged["rows"]], ["$1", "$5", "$1"])

    def test_empty_source_table_keeps_paid_history(self):
        old = divs(("2024-06-01", "$1", None, None))
        merged, _ = self.merge(old, {"headers": [], "rows": []})
        self.assertIs(merged, old)

    def test_header_union_fills_missing_cells_with_none(self):
        old = divs(("2020-06-01", "$1", None, None))
        new = divs(
            ("2026-06-01", "$2", "2026-05-01", None, None),
            headers=(
                "Ex-Dividend Date",
                "Cash Amount",
                "Declared Date",
                "Record Date",
                "Pay Date",
            ),
        )
        merged, _ = self.merge(old, new)
        self.assertEqual(
            merged["headers"],
            [
                "Ex-Dividend Date",
                "Cash Amount",
                "Declared Date",
                "Record Date",
                "Pay Date",
            ],
        )
        self.assertIsNone(merged["rows"][1]["Declared Date"])

    def test_split_adjusted_history_rebases_retained_amounts(self):
        old = divs(
            ("2026-03-01", "0.630 AED", None, None),
            ("2025-09-01", "0.600 AED", None, None),
            ("2021-03-01", "0.25663 AED", None, None),
        )
        new = divs(
            ("2026-06-01", "0.320 AED", None, None),
            ("2026-03-01", "0.315 AED", None, None),
            ("2025-09-01", "0.300 AED", None, None),
        )
        merged, _ = self.merge(old, new)
        self.assertEqual(merged["rows"][-1]["Cash Amount"], "0.128315 AED")

    def test_currency_change_does_not_rebase(self):
        old = divs(
            ("2026-03-01", "$1.00", None, None),
            ("2025-09-01", "$1.00", None, None),
            ("2021-03-01", "$0.50", None, None),
        )
        new = divs(
            ("2026-03-01", "£0.80", None, None), ("2025-09-01", "£0.80", None, None)
        )
        merged, _ = self.merge(old, new)
        self.assertEqual(merged["rows"][-1]["Cash Amount"], "$0.50")


class DocumentMergeTests(unittest.TestCase):
    def setUp(self):
        self.old = {
            "ticker": "NYSE:ABC",
            "overview": {"symbol": "ABC", "stats": {"Market Cap": "1B", "Old": "x"}},
            "statistics": {"sections": {"Scores": {"Altman Z": "3"}}},
            "financials": table("TTM", [2025, 2024], {"Revenue": ["9", "3", "2"]}),
            "dividends": divs(("2021-06-01", "$1", None, None)),
            "purchase_details": [],
        }

    def test_snapshots_replace_and_history_accumulates(self):
        new = copy.deepcopy(self.old)
        new["overview"] = {"symbol": "ABC", "stats": {"Market Cap": "2B"}}
        new["statistics"] = {"sections": {"Margins": {"Gross": "10%"}}}
        new["financials"] = table("TTM", [2026, 2025], {"Revenue": ["9", "4", "3"]})
        new["dividends"] = divs(("2026-06-01", "$2", None, None))
        merged, _ = merge_ticker(self.old, new)
        self.assertEqual(merged["overview"], new["overview"])
        self.assertEqual(merged["statistics"], new["statistics"])
        self.assertIn("FY 2024", merged["financials"]["headers"])
        self.assertEqual(len(merged["dividends"]["rows"]), 2)

    def test_merge_is_idempotent_and_does_not_mutate_inputs(self):
        new = copy.deepcopy(self.old)
        new["financials"] = table("TTM", [2026, 2025], {"Revenue": ["9", "4", "3"]})
        snapshot_old, snapshot_new = copy.deepcopy(self.old), copy.deepcopy(new)
        once, _ = merge_ticker(self.old, new)
        twice, _ = merge_ticker(once, copy.deepcopy(new))
        self.assertEqual(once, twice)
        self.assertEqual(merge_ticker(once, copy.deepcopy(once))[0], once)
        self.assertEqual((self.old, new), (snapshot_old, snapshot_new))

    def test_failed_or_missing_sections_keep_last_good_copy(self):
        merged, _ = merge_ticker(
            self.old,
            {"overview": {"symbol": "ABC"}, "financials": {"error": "timeout"}},
        )
        self.assertEqual(merged["financials"], self.old["financials"])
        self.assertEqual(merged["dividends"], self.old["dividends"])


class CacheSaveMergeTests(unittest.TestCase):
    def test_save_merges_and_archives_superseded_history(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Cache(Path(directory), read_only=False)
            labels = {f"Item {i}": ["1", "100", "100", "100"] for i in range(4)}
            cache.save(
                "NYSE:ABC",
                {
                    "overview": {"symbol": "ABC"},
                    "financials": table("TTM", [2025, 2024, 2023], labels),
                },
            )
            rebased = {k: ["1", "9", "27", "31"] for k in labels}
            cache.save(
                "NYSE:ABC",
                {
                    "overview": {"symbol": "ABC"},
                    "financials": table("TTM", [2026, 2025, 2024], rebased),
                },
            )
            saved = cache.load("NYSE:ABC")
            self.assertNotIn("FY 2023", saved["financials"]["headers"])
            archived = list(
                (Path(directory) / "archive").glob("nyse_abc.financials.*.json")
            )
            self.assertEqual(len(archived), 1)
            self.assertIn("FY 2023", json.loads(archived[0].read_text())["headers"])
            self.assertEqual(set(cache.status()), {"nyse:abc"})


if __name__ == "__main__":
    unittest.main()
