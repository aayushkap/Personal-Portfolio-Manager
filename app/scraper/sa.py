"""
Scrapes overview, financials, dividends, statistics, and ratios for given tickers.
Returns all data as a single nested dictionary.
"""

import asyncio
import random
import re
from typing import Dict, Any
from dateutil import parser
import logging
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from playwright_stealth import stealth_async
from app.utils.time_utils import dubai_now_iso

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class StockAnalysisScraper:
    """
    Scraper for StockAnalysis.com that collects financial data for a list of tickers.
    """

    # Common user agents and viewports for fingerprint rotation
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    ]

    VIEWPORTS = [
        {"width": 1920, "height": 1080},
        {"width": 1440, "height": 900},
        {"width": 1536, "height": 864},
    ]

    def __init__(
        self,
        headless: bool = True,
        timeout: int = 45000,
        max_retries: int = 3,
    ):
        """
        :param headless: Whether to run browser in headless mode
        :param timeout: Navigation timeout in milliseconds
        :param max_retries: Number of retries for failed navigations
        """
        self.headless = headless
        self.timeout = timeout
        self.max_retries = max_retries

    # Human‑like behaviour helpers
    @staticmethod
    async def _jitter(lo: float = 0.4, hi: float = 1.8) -> None:
        """Random pause to simulate human reading speed."""
        await asyncio.sleep(random.uniform(lo, hi))

    @staticmethod
    async def _human_scroll(page: Page, passes: int = 4) -> None:
        """Scroll down gradually, then back to top."""
        for _ in range(passes):
            delta = random.randint(250, 550)
            await page.evaluate(f"window.scrollBy(0, {delta})")
            await asyncio.sleep(random.uniform(0.25, 0.65))
        await asyncio.sleep(random.uniform(0.4, 0.9))
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(random.uniform(0.2, 0.5))

    @staticmethod
    async def _human_mouse_wander(page: Page) -> None:
        """Idle mouse movement before scraping."""
        vp = page.viewport_size or {"width": 1280, "height": 800}
        for _ in range(random.randint(3, 6)):
            x = random.randint(80, vp["width"] - 80)
            y = random.randint(80, vp["height"] - 200)
            await page.mouse.move(x, y, steps=random.randint(8, 20))
            await asyncio.sleep(random.uniform(0.08, 0.25))

    async def _safe_goto(
        self, page: Page, url: str, wait_for: str = "domcontentloaded"
    ) -> None:
        """Navigate with retries and human pacing."""
        for attempt in range(self.max_retries):
            try:
                await page.goto(url, wait_until=wait_for, timeout=self.timeout)
                # await page.screenshot(f"{url}.png")
                await self._jitter(1.0, 2)
                return
            except Exception as exc:
                if attempt == self.max_retries - 1:
                    raise
                wait = random.uniform(3, 7)
                print(
                    f"  Retry {attempt + 1}/{self.max_retries} for {url} — {exc} — waiting {wait:.1f}s"
                )
                await asyncio.sleep(wait)

    # Context creation with stealth and fingerprint spoofing
    async def _create_context(self, browser: Browser) -> BrowserContext:
        """Create a new browser context with random user agent and viewport, plus stealth init scripts."""
        ua = random.choice(self.USER_AGENTS)
        vp = random.choice(self.VIEWPORTS)

        context = await browser.new_context(
            user_agent=ua,
            viewport=vp,
            locale="en-US",
            timezone_id="Asia/Dubai",
            java_script_enabled=True,
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.google.com/",
            },
        )

        # Spoof additional fingerprint signals
        await context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            window.chrome = { runtime: {} };
            """
        )

        return context

    # Page‑specific scraping methods
    async def _scrape_overview(
        self, page: Page, exchange: str, symbol: str
    ) -> Dict[str, Any]:
        """Scrape the overview page (price, change, key stats)."""
        url = f"https://stockanalysis.com/quote/{exchange}/{symbol}/"

        logger.info(f"Scraping overview for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, url)
        await self._human_mouse_wander(page)
        await self._human_scroll(page)

        data = {
            "symbol": symbol,
            "exchange": exchange.upper(),
            # "url": url,
            # "scraped_at": dubai_now_iso(),
        }

        # Price and change
        price_el = await page.query_selector("[data-test='quote-price']")
        if price_el:
            data["price"] = (await price_el.inner_text()).strip()
        change_el = await page.query_selector("[data-test='quote-change']")
        if change_el:
            data["price_change"] = (await change_el.inner_text()).strip()

        # Summary stats table
        stats = {}
        try:
            rows = await page.query_selector_all(
                "table tbody tr, [class*='snapshot'] tr"
            )
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    key = (await cells[0].inner_text()).strip().rstrip(":")
                    value = (await cells[1].inner_text()).strip()
                    if key:
                        stats[key] = value
        except Exception:
            pass

        # Fallback to dl/dt/dd
        try:
            dts = await page.query_selector_all("dt")
            dds = await page.query_selector_all("dd")
            for dt, dd in zip(dts, dds):
                k = (await dt.inner_text()).strip()
                v = (await dd.inner_text()).strip()
                if k:
                    stats[k] = v
        except Exception:
            pass

        data["stats"] = stats
        await self._jitter(0.8, 1.5)
        return data

    async def _scrape_financials(
        self, page: Page, exchange: str, symbol: str
    ) -> Dict[str, Any]:
        """Scrape the financials table (income statement / balance sheet)."""
        url = f"https://stockanalysis.com/quote/{exchange}/{symbol}/financials/"

        logger.info(f"Scraping financials for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, url)
        await self._human_mouse_wander(page)

        try:
            await page.wait_for_selector("#main-table", timeout=20000)
        except Exception:
            await page.wait_for_selector("table", timeout=5000)

        await self._human_scroll(page, passes=5)
        await self._jitter(0.8, 1.25)

        headers = []
        rows = []

        try:
            header_cells = await page.query_selector_all(
                "#main-table thead tr:first-child th"
            )
            headers = [(await c.inner_text()).strip() for c in header_cells]

            data_rows = await page.query_selector_all("#main-table tbody tr")
            for row in data_rows:
                cells = await row.query_selector_all("td")
                values = [(await c.inner_text()).strip() for c in cells]
                if values:
                    row_dict = {}
                    for i, val in enumerate(values):
                        col_name = headers[i] if i < len(headers) else f"col_{i}"
                        row_dict[col_name] = val
                    rows.append(row_dict)
        except Exception as exc:
            rows = [{"error": str(exc)}]

        result = {
            "symbol": symbol,
            "exchange": exchange.upper(),
            "url": url,
            "scraped_at": dubai_now_iso(),
            "headers": headers,
            "rows": rows,
        }
        await self._jitter(1.0, 2)
        return result

    async def _scrape_dividends(
        self, page: Page, exchange: str, symbol: str
    ) -> Dict[str, Any]:
        """Scrape the dividend history table."""
        url = f"https://stockanalysis.com/quote/{exchange}/{symbol}/dividend/"

        logger.info(f"Scraping dividends for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, url)
        await self._human_mouse_wander(page)

        try:
            await page.wait_for_selector(".table-wrap table", timeout=20000)
        except Exception:
            await page.wait_for_selector("table", timeout=15000)

        await self._human_scroll(page, passes=4)
        await self._jitter(0.6, 1.2)

        headers = []
        rows = []

        try:
            th_els = await page.query_selector_all(".table-wrap table thead th")
            headers = [(await th.inner_text()).strip() for th in th_els]

            tr_els = await page.query_selector_all(".table-wrap table tbody tr")
            for tr in tr_els:
                tds = await tr.query_selector_all("td")
                values = [(await td.inner_text()).strip() for td in tds]

                if values:
                    row_dict = {}
                    for i, v in enumerate(values):
                        col = headers[i] if i < len(headers) else f"col_{i}"

                        if col in {"Ex-Dividend Date", "Record Date", "Pay Date"}:
                            try:
                                v = parser.parse(v).date().isoformat()
                            except Exception:
                                pass

                        row_dict[col] = v

                    rows.append(row_dict)

        except Exception as exc:
            rows = [{"error": str(exc)}]

        result = {
            "symbol": symbol,
            "exchange": exchange.upper(),
            "url": url,
            "scraped_at": dubai_now_iso(),
            "headers": headers,
            "rows": rows,
        }

        await self._jitter(0.8, 1.5)
        return result

    async def _scrape_statistics(
        self, page: Page, exchange: str, symbol: str
    ) -> Dict[str, Any]:
        """Scrape the statistics page (detailed ratios and metrics)."""
        url = f"https://stockanalysis.com/quote/{exchange}/{symbol}/statistics/"

        logger.info(f"Scraping statistics for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, url)
        await self._human_mouse_wander(page)

        try:
            await page.wait_for_selector("h2", timeout=20000)
        except Exception:
            pass

        await self._human_scroll(page, passes=5)
        await self._jitter(0.8, 1.4)

        data = {
            "symbol": symbol,
            "exchange": exchange.upper(),
            "url": url,
            "scraped_at": dubai_now_iso(),
            "sections": {},
        }

        try:
            h2_elements = await page.query_selector_all("h2")
            for h2 in h2_elements:
                section_name = (await h2.inner_text()).strip()
                if not section_name:
                    continue

                parent = await h2.evaluate_handle("el => el.parentElement")
                table = await parent.query_selector("table")
                if not table:
                    continue

                rows = await table.query_selector_all("tbody tr")
                section_data = {}
                for row in rows:
                    tds = await row.query_selector_all("td")
                    if len(tds) < 2:
                        continue

                    key = (await tds[0].inner_text()).strip().rstrip(":")
                    raw = await tds[1].get_attribute("title") or ""
                    disp = (await tds[1].inner_text()).strip()
                    value = raw.strip() if raw.strip() and raw.strip() != disp else disp

                    if key:
                        section_data[key] = raw.strip() or disp

                if section_data:
                    data["sections"][section_name] = section_data

            # Flat views
            data["all_stats"] = {
                k: v["raw"]
                for section in data["sections"].values()
                for k, v in section.items()
            }

            # Filter for return metrics
            RETURN_KEYWORDS = ("ROE", "ROA", "ROIC", "ROCE", "RETURN ON", "WACC")
            data["return_metrics"] = {
                k: v
                for k, v in data["all_stats"].items()
                if any(kw in k.upper() for kw in RETURN_KEYWORDS)
            }

        except Exception as exc:
            data["error"] = str(exc)

        await self._jitter(1.0, 1.5)
        return data

    async def _scrape_ratios(
        self, page: Page, exchange: str, symbol: str
    ) -> Dict[str, Any]:
        """Scrape the financial ratios page (historical time series)."""
        url = f"https://stockanalysis.com/quote/{exchange}/{symbol}/financials/ratios/"

        logger.info(f"Scraping ratios for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, url)
        await self._human_mouse_wander(page)

        try:
            await page.wait_for_selector("#main-table", timeout=20000)
        except Exception:
            await page.wait_for_selector("table", timeout=15000)

        await self._human_scroll(page, passes=5)
        await self._jitter(0.8, 1.25)

        headers = []
        rows = []

        try:
            header_cells = await page.query_selector_all(
                "#main-table thead tr:first-child th"
            )
            headers = [(await c.inner_text()).strip() for c in header_cells]

            data_rows = await page.query_selector_all("#main-table tbody tr")
            for row in data_rows:
                cells = await row.query_selector_all("td")
                values = [(await c.inner_text()).strip() for c in cells]
                if values:
                    row_dict = {}
                    for i, val in enumerate(values):
                        col_name = headers[i] if i < len(headers) else f"col_{i}"
                        row_dict[col_name] = val
                    rows.append(row_dict)
        except Exception as exc:
            rows = [{"error": str(exc)}]

        # Filter rows containing return metrics
        return_rows = [
            r
            for r in rows
            if any(
                kw in r.get(headers[0] if headers else "col_0", "").upper()
                for kw in ("ROE", "ROA", "ROIC", "ROCE", "RETURN")
            )
        ]

        result = {
            "symbol": symbol,
            "exchange": exchange.upper(),
            "url": url,
            "scraped_at": dubai_now_iso(),
            "headers": headers,
            "rows": rows,
            "return_metrics_rows": return_rows,
        }
        await self._jitter(1.0, 2)
        return result

    # Ticker‑level orchestration
    async def _scrape_ticker(
        self, browser: Browser, ticker: Dict[str, str]
    ) -> Dict[str, Any]:
        exchange = ticker["exchange"]
        symbol = ticker["symbol"]
        key = f"{exchange.upper()}:{symbol}"

        context = await self._create_context(browser)
        page = await context.new_page()
        await stealth_async(page)

        await page.route(
            "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,mp4,webm}",
            lambda route: route.abort(),
        )
        await page.route(
            re.compile(
                r"(google-analytics|googletagmanager|facebook\.net|doubleclick)"
            ),
            lambda route: route.abort(),
        )

        result = {"ticker": key, "scraped_at": dubai_now_iso()}

        #  Each section is independent — one failure never blocks the others
        sections = [
            ("overview", lambda: self._scrape_overview(page, exchange, symbol)),
            ("financials", lambda: self._scrape_financials(page, exchange, symbol)),
            ("dividends", lambda: self._scrape_dividends(page, exchange, symbol)),
            ("statistics", lambda: self._scrape_statistics(page, exchange, symbol)),
            ("ratios", lambda: self._scrape_ratios(page, exchange, symbol)),
        ]

        for section_name, scrape_fn in sections:
            try:
                result[section_name] = await scrape_fn()
                if section_name != "ohlc":
                    await self._jitter(1.5, 2.0)
            except Exception as exc:
                print(f"  [SKIP] {key} › {section_name}: {exc}")
                result[section_name] = {
                    "error": str(exc)
                }  # section failed, not the ticker

        # Ticker is only fully failed if EVERY section errored
        all_failed = all(
            isinstance(result.get(s), dict) and "error" in result.get(s, {})
            for s in ("overview", "financials", "dividends", "statistics", "ratios")
        )
        if all_failed:
            result["error"] = "all sections failed"

        try:
            await context.close()
        except Exception:
            pass

        cooldown = random.uniform(15.0, 30.0)
        print(f"Cooling down {cooldown:.1f}s …")
        await asyncio.sleep(cooldown)

        return result

    # Public API
    async def scrape(self, ticker: dict) -> Dict[str, Any]:
        """
        Scrape tickers and return a dictionary with results. The dictionary has ticker keys (e.g. 'DFM:DEWA') containing the scraped data.
        """

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=self.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-accelerated-2d-canvas",
                    "--no-first-run",
                    "--no-zygote",
                    "--disable-gpu",
                ],
            )

            ticker_result = await self._scrape_ticker(browser, ticker)
            await browser.close()

        return ticker_result


async def main():
    obj = StockAnalysisScraper()
    res = await obj.scrape({"exchange": "DFM", "symbol": "EMAAR"})
    import json

    with open("filename.json", "w") as f:
        json.dump(res, f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
