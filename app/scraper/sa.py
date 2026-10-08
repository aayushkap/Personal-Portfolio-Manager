"""
Scrapes overview, financials, dividends, statistics, and ratios for given tickers.
Returns all data as a single nested dictionary.
"""

import asyncio
import contextlib
import os
import random
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import AsyncGenerator, Dict, Any
from dateutil import parser
import requests
from bs4 import BeautifulSoup

# Patchright is a drop-in Playwright build without the CDP automation leaks
# (Runtime.enable and friends) that Cloudflare's challenge script detects.
# Stock Playwright never clears the StockAnalysis challenge, even headful.
from patchright.async_api import (
    async_playwright,
    BrowserContext,
    Page,
    Playwright,
    Response,
)
from app.config import (
    STOCKANALYSIS_BROWSER_PROFILE_DIR,
    STOCKANALYSIS_DEBUG_SCREENSHOT_DIR,
    STOCKANALYSIS_HEADLESS,
)
from app.utils.time_utils import dubai_now_iso

from app.core.logger import get_logger

logger = get_logger()

_BROWSER_CACHE_CLEANUP_INTERVAL_SECONDS = 48 * 60 * 60


class SourcePageUnavailable(RuntimeError):
    """The source returned a missing, blocked, or anti-bot page."""

    def __init__(self, message: str, *, source_blocked: bool = False) -> None:
        super().__init__(message)
        self.source_blocked = source_blocked


def retriable(retries: int = 2, delay: float = 30.0):
    def decorator(fn):
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(retries + 1):
                try:
                    return await fn(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if isinstance(exc, SourcePageUnavailable) and exc.source_blocked:
                        raise
                    if attempt < retries:
                        logger.warning(
                            "%s attempt %d failed: %s — retrying in %.0fs",
                            fn.__name__,
                            attempt + 1,
                            exc,
                            delay,
                        )
                        await asyncio.sleep(delay)
            raise last_exc

        return wrapper

    return decorator


class StockAnalysisScraper:
    """
    Scraper for StockAnalysis.com that collects financial data for a list of tickers.
    """

    def __init__(
        self,
        headless: bool | None = None,
        timeout: int = 45000,
        max_retries: int = 3,
    ):
        """
        :param headless: Whether to run browser in headless mode.  ``None``
            uses the STOCKANALYSIS_HEADLESS setting.
        :param timeout: Navigation timeout in milliseconds
        :param max_retries: Number of retries for failed navigations
        """
        self.headless = STOCKANALYSIS_HEADLESS if headless is None else headless
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
        # The browser window is not emulated, so read its real dimensions.
        vp = await page.evaluate(
            "() => ({width: window.innerWidth, height: window.innerHeight})"
        )
        if vp["width"] < 400 or vp["height"] < 400:
            vp = {"width": 1280, "height": 800}
        for _ in range(random.randint(3, 6)):
            x = random.randint(80, vp["width"] - 80)
            y = random.randint(80, vp["height"] - 200)
            await page.mouse.move(x, y, steps=random.randint(8, 20))
            await asyncio.sleep(random.uniform(0.08, 0.25))

    @staticmethod
    def _to_iso_date(value: str) -> str | None:
        """Normalise any human-readable date string to YYYY-MM-DD. Returns None for missing/invalid."""
        if not value or value.strip() in {"-", "—", "N/A", "None", ""}:
            return None
        try:
            return parser.parse(value.strip()).date().isoformat()
        except Exception:
            return None

    def _get_base_url(self, exchange: str, symbol: str, is_etf: bool = False) -> str:
        """Determine the Stock Analysis URL for a stock, ETF, or foreign quote."""
        # StockAnalysis' /etf/ URLs are for US-listed funds.  Foreign ETFs
        # such as LON:EIMI and LON:XUSE live under their exchange quote URL,
        # even though their source metadata correctly classifies them as ETFs.
        us_exchanges = {"NYSE", "NASDAQ", "AMEX", "OTC", "BATS"}
        if is_etf and exchange.upper() in us_exchanges:
            return f"https://stockanalysis.com/etf/{symbol.lower()}"

        if exchange.upper() in us_exchanges:
            return f"https://stockanalysis.com/stocks/{symbol.lower()}"
        return f"https://stockanalysis.com/quote/{exchange.lower()}/{symbol.lower()}"

    async def _save_failure_screenshot(
        self, page: Page, url: str, response: Response | None = None
    ) -> Path | None:
        """Persist the rendered response for a failed source navigation.

        Chromium can capture a page in headless mode, including a 403/challenge
        response. Wait for the renderer to paint first, then retain its HTML as
        well: an edge response can otherwise be recorded as a blank frame even
        when it contains useful diagnostic markup.
        """
        try:
            safe_url = re.sub(r"[^a-z0-9]+", "-", url.lower()).strip("-")
            filename = f"{dubai_now_iso().replace(':', '-')}-{safe_url[:120]}.png"
            path = STOCKANALYSIS_DEBUG_SCREENSHOT_DIR / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            await page.wait_for_timeout(1_000)
            await page.screenshot(path=str(path), full_page=True, timeout=15_000)
            html_path = path.with_suffix(".html")
            html_path.write_text(await page.content(), encoding="utf-8")
            logger.warning(
                "Saved StockAnalysis failure artifacts: screenshot=%s html=%s status=%s",
                path,
                html_path,
                response.status if response else "unavailable",
            )
            return path
        except Exception:
            logger.exception(
                "Could not save StockAnalysis failure screenshot for %s", url
            )
            return None

    @staticmethod
    def _has_etf_tag(tags: Any) -> bool:
        """Return whether ``tags`` contains ETF as a distinct, case-insensitive tag."""
        if isinstance(tags, str):
            values = tags.split("+")
        elif isinstance(tags, (list, tuple, set)):
            values = tags
        else:
            return False
        return any(str(tag).strip().upper() == "ETF" for tag in values)

    @staticmethod
    def _overview_confirms_no_dividends(overview: Any) -> bool:
        """Return true only for an explicit no-dividend signal from the source.

        A missing overview value is not enough evidence: it can result from a
        challenge page or a partial load and must retain normal retry behavior.
        """
        if not isinstance(overview, dict):
            return False
        stats = overview.get("stats")
        if not isinstance(stats, dict):
            return False

        no_dividend_values = {"n/a", "na", "not applicable", "none"}
        for field in ("Dividend (ttm)", "Dividend", "Dividend Yield"):
            value = stats.get(field)
            if isinstance(value, str) and value.strip().lower() in no_dividend_values:
                return True
        return False

    @staticmethod
    def _section_is_usable(section_name: str, section: Any) -> bool:
        """Whether a section contains source data rather than a failed shell."""
        if not isinstance(section, dict) or section.get("error"):
            return False

        if section_name == "overview":
            return bool(section.get("symbol")) and bool(section.get("stats"))
        if section_name == "statistics":
            return bool(section.get("sections"))

        rows = section.get("rows")
        if not isinstance(rows, list):
            return False
        return not any(isinstance(row, dict) and row.get("error") for row in rows)

    @classmethod
    def has_usable_result(cls, result: Any) -> bool:
        """Require a valid overview plus one other successful source section."""
        if not isinstance(result, dict) or result.get("error"):
            return False

        if not cls._section_is_usable("overview", result.get("overview")):
            return False

        section_names = ("dividends", "financials", "statistics", "ratios")
        return any(
            cls._section_is_usable(section_name, result.get(section_name))
            for section_name in section_names
        )

    @staticmethod
    def _empty_dividends(exchange: str, symbol: str, url: str) -> Dict[str, Any]:
        """Build a successful, intentionally empty dividend-table result."""
        return {
            "symbol": symbol,
            "exchange": exchange.upper(),
            "url": url,
            "scraped_at": dubai_now_iso(),
            "headers": [],
            "rows": [],
        }

    @staticmethod
    def _parse_dividend_html(html: str) -> tuple[list[str], list[dict[str, str]]]:
        """Extract StockAnalysis's server-rendered dividend table."""
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one(".table-wrap table")
        if table is None:
            page_text = soup.get_text(" ", strip=True).lower()
            no_history_markers = (
                "there is no dividend history available",
                "has never paid a dividend",
                "does not pay a dividend",
            )
            if any(marker in page_text for marker in no_history_markers):
                return [], []
            title = soup.title.get_text(" ", strip=True) if soup.title else "untitled"
            raise RuntimeError(f"dividend table absent from HTML ({title})")

        headers = [
            StockAnalysisScraper._normalise_dividend_header(
                cell.get_text(" ", strip=True)
            )
            for cell in table.select("thead th")
        ]
        if not headers:
            raise RuntimeError("dividend table has no headers")

        rows = []
        for tr in table.select("tbody tr"):
            values = [cell.get_text(" ", strip=True) for cell in tr.select("td")]
            if values:
                rows.append(
                    {
                        headers[index]
                        if index < len(headers)
                        else f"col_{index}": value
                        for index, value in enumerate(values)
                    }
                )
        return headers, rows

    @staticmethod
    def _normalise_dividend_header(value: str) -> str:
        """Restore canonical table headings split by nested source markup."""
        compact = re.sub(r"[^a-z0-9]", "", value.lower())
        canonical = {
            "exdividenddate": "Ex-Dividend Date",
            "cashamount": "Cash Amount",
            "recorddate": "Record Date",
            "paydate": "Pay Date",
        }
        return canonical.get(compact, " ".join(value.split()))

    @classmethod
    def _fetch_dividend_html(
        cls, url: str
    ) -> tuple[list[str], list[dict[str, str]]] | None:
        """Fetch the static table, returning ``None`` for an actual 404 page."""
        response = requests.get(
            url,
            headers={
                # Retained only for compatibility with one-off callers.  The
                # scheduled scraper uses the persistent browser profile below.
                "User-Agent": "Mozilla/5.0",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=(10, 30),
        )
        if response.status_code == 404:
            return None
        if response.status_code in {403, 429}:
            raise SourcePageUnavailable(
                f"source returned HTTP {response.status_code} for {url}",
                source_blocked=True,
            )
        response.raise_for_status()
        # StockAnalysis serves UTF-8 markup without a sufficiently explicit
        # charset for requests.  Without this, a pound sign becomes ``Â£`` and
        # the currency parser drops otherwise valid UK dividends.
        response.encoding = "utf-8"
        return cls._parse_dividend_html(response.text)

    async def _safe_goto(
        self, page: Page, url: str, wait_for: str = "domcontentloaded"
    ) -> Response:
        """Navigate with retries and reject missing or anti-bot source pages."""
        for attempt in range(self.max_retries):
            response: Response | None = None
            try:
                response = await page.goto(
                    url, wait_until=wait_for, timeout=self.timeout
                )
                if response is None:
                    raise SourcePageUnavailable(f"no navigation response for {url}")
                if await self._is_challenge_page(page):
                    # Cloudflare serves its check with HTTP 403.  A real
                    # browser clears it unattended within a few seconds and
                    # reloads the requested page with a clearance cookie.
                    logger.info("Cloudflare check on %s — waiting for it to clear", url)
                    if not await self._wait_for_challenge(page):
                        raise SourcePageUnavailable(
                            f"Cloudflare challenge did not clear for {url}",
                            source_blocked=True,
                        )
                    logger.info("Cloudflare check cleared for %s", url)
                elif response.status >= 400:
                    raise SourcePageUnavailable(
                        f"source returned HTTP {response.status} for {url}",
                        source_blocked=response.status in {403, 429},
                    )

                title = (await page.title()).strip().lower()
                body = (await page.locator("body").inner_text(timeout=5000)).lower()
                blocked_markers = (
                    "access denied",
                    "captcha",
                    "verify you are human",
                    "just a moment",
                    "unusual traffic",
                )
                missing_title_markers = ("404", "page not found", "not found")
                if any(marker in body for marker in blocked_markers) or any(
                    marker in title for marker in missing_title_markers
                ):
                    raise SourcePageUnavailable(
                        f"source page unavailable for {url}",
                        source_blocked=any(
                            marker in body for marker in blocked_markers
                        ),
                    )

                # await page.screenshot(f"{url}.png")
                await self._jitter(1.0, 2)
                return response
            except Exception as exc:
                if isinstance(exc, SourcePageUnavailable):
                    await self._save_failure_screenshot(page, url, response)
                if isinstance(exc, SourcePageUnavailable) and exc.source_blocked:
                    raise
                if attempt == self.max_retries - 1:
                    raise
                wait = random.uniform(3, 7)
                logger.info(
                    f"  Retry {attempt + 1}/{self.max_retries} for {url} — {exc} — waiting {wait:.1f}s"
                )
                await asyncio.sleep(wait)

    @staticmethod
    async def _is_challenge_page(page: Page) -> bool:
        """Whether the page is Cloudflare's "Just a moment..." interstitial."""
        try:
            title = (await page.title()).strip().lower()
            if "just a moment" in title:
                return True
            return await page.locator("#challenge-error-text").count() > 0
        except Exception:
            return False

    async def _wait_for_challenge(self, page: Page, timeout: int = 45000) -> bool:
        """Let the browser clear a Cloudflare check; True once the real page loads."""
        try:
            await page.wait_for_function(
                "() => !document.title.toLowerCase().includes('just a moment')"
                " && !document.querySelector('#challenge-error-text')",
                timeout=timeout,
            )
            await page.wait_for_load_state("domcontentloaded", timeout=self.timeout)
        except Exception:
            return False
        return not await self._is_challenge_page(page)

    @staticmethod
    @contextlib.asynccontextmanager
    async def _virtual_display(enabled: bool) -> AsyncGenerator[dict[str, str], None]:
        """Provide an X display for a headful browser on a monitor-less server.

        Cloudflare rejects headless Chromium regardless of its headers, so the
        browser runs with a real window.  Linux servers without ``DISPLAY`` get
        a private Xvfb screen for the browser only; desktops use their own.
        """
        env = dict(os.environ)
        if not enabled or sys.platform != "linux" or env.get("DISPLAY"):
            yield env
            return
        if shutil.which("Xvfb") is None:
            raise RuntimeError(
                "Xvfb is required for the headful StockAnalysis browser; "
                "install it (apt install xvfb) or set STOCKANALYSIS_HEADLESS=true"
            )

        read_fd, write_fd = os.pipe()
        xvfb = subprocess.Popen(
            [
                "Xvfb",
                "-displayfd",
                str(write_fd),
                "-screen",
                "0",
                "1920x1080x24",
                "-nolisten",
                "tcp",
            ],
            pass_fds=(write_fd,),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.close(write_fd)
        try:
            with os.fdopen(read_fd) as display_pipe:
                display = await asyncio.wait_for(
                    asyncio.to_thread(display_pipe.readline), timeout=15
                )
            if not display.strip():
                raise RuntimeError("Xvfb exited before reporting a display")
            env["DISPLAY"] = f":{display.strip()}"
            yield env
        finally:
            xvfb.terminate()
            try:
                xvfb.wait(timeout=5)
            except subprocess.TimeoutExpired:
                xvfb.kill()

    async def _launch_context(
        self, pw: Playwright, env: dict[str, str]
    ) -> BrowserContext:
        """Launch the persistent profile as an unmodified, real browser.

        Do not add stealth scripts, a user-agent override, or viewport
        emulation: each makes the JavaScript fingerprint disagree with the
        browser binary, which is precisely what Cloudflare checks for.
        """
        return await pw.chromium.launch_persistent_context(
            str(STOCKANALYSIS_BROWSER_PROFILE_DIR),
            headless=self.headless,
            no_viewport=True,
            env=env,
            timeout=self.timeout,
        )

    @staticmethod
    def _clear_browser_cache_if_due() -> None:
        """Clear disposable caches daily, only after the browser closes."""
        profile = STOCKANALYSIS_BROWSER_PROFILE_DIR
        marker = profile / ".cache-cleaned-at"
        try:
            # Chromium owns the profile while this lock exists. Even dangling
            # locks are left alone rather than risking a live browser's files.
            lock = profile / "SingletonLock"
            if lock.exists() or lock.is_symlink():
                return
            if not profile.is_dir() or (profile / "Default").is_symlink():
                return
            if marker.exists() and (
                time.time() - marker.stat().st_mtime
                < _BROWSER_CACHE_CLEANUP_INTERVAL_SECONDS
            ):
                return
            for name in ("Cache", "Code Cache"):
                cache = profile / "Default" / name
                if cache.is_dir() and not cache.is_symlink():
                    shutil.rmtree(cache)
            marker.touch()
            logger.info("Cleared daily StockAnalysis browser caches in %s", profile)
        except OSError:
            # Cleanup must not discard a completed scrape; retry next session.
            logger.warning(
                "Could not clear StockAnalysis browser caches", exc_info=True
            )

    # Page‑specific scraping methods
    @staticmethod
    def _quote_currency_from_text(text: str) -> str | None:
        # London listings may say "Currency is GBP · Price in GBX" or USD.
        match = re.search(r"\bPrice in\s+([A-Z]{3})\b", text)
        if match is None:
            match = re.search(r"\bCurrency is\s+([A-Z]{3})\b", text)
        return match.group(1) if match else None

    @retriable(retries=2, delay=30.0)
    async def _scrape_overview(
        self, page: Page, exchange: str, symbol: str, is_etf: bool = False
    ) -> Dict[str, Any]:
        """Scrape the overview page (price, change, key stats)."""
        url = f"{self._get_base_url(exchange, symbol, is_etf)}/"

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

        quote_currency = self._quote_currency_from_text(
            await page.locator("body").inner_text()
        )
        if quote_currency:
            data["quote_currency"] = quote_currency

        # Price and change
        price_el = await page.query_selector("[data-test='quote-price']")
        if price_el:
            data["price"] = (await price_el.inner_text()).strip()
        change_el = await page.query_selector("[data-test='quote-change']")
        if change_el:
            data["price_change"] = (await change_el.inner_text()).strip()

        # Company summary in the "About <symbol>" section on the overview page.
        # Remove the nested "[Read more]" link so only the summary is stored.
        try:
            about = await page.evaluate("""() => {
                    const heading = [...document.querySelectorAll('h2')].find(
                        (element) => /^About\\s+/i.test(element.textContent.trim())
                    );
                    const paragraph = heading?.parentElement?.querySelector('p');
                    if (!paragraph) return null;
                    const copy = paragraph.cloneNode(true);
                    copy.querySelectorAll('a').forEach((link) => link.remove());
                    return copy.textContent.replace(/\\s+/g, ' ').trim() || null;
                }""")
            if about:
                data["about"] = about
        except Exception:
            pass

        try:
            sector = await page.evaluate("""() => {
                    const grid = [...document.querySelectorAll('div.grid')].find(
                        (g) => [...g.querySelectorAll('span.font-semibold')]
                            .some((s) => /^Sector$/i.test(s.textContent.trim()))
                        || [...g.querySelectorAll('span.font-semibold')]
                            .some((s) => /^Industry$/i.test(s.textContent.trim()))
                    );
                    if (!grid) return null;

                    const getValueFor = (label) => {
                        const span = [...grid.querySelectorAll('span.font-semibold')].find(
                            (s) => new RegExp('^' + label + '$', 'i').test(s.textContent.trim())
                        );
                        if (!span) return null;
                        const container = span.closest('div.col-span-1') || span.parentElement;
                        const link = container?.querySelector('a');
                        const fallbackSpan = container?.querySelector('span:not(.font-semibold)');
                        const raw = (link || fallbackSpan)?.textContent?.trim();
                        return raw || null;
                    };

                    let value = getValueFor('Sector');
                    let usedIndustryFallback = false;

                    if (!value) {
                        value = getValueFor('Industry');
                        usedIndustryFallback = true;
                    }

                    if (!value) return null;

                    if (usedIndustryFallback && value.includes('-')) {
                        value = value.split('-')[0].trim();
                    }

                    return value.replace(/\\s+/g, ' ').trim() || null;
                }""")
            if sector:
                data["sector"] = sector
        except Exception:
            pass

        print(f"Data scraped for {exchange}:{symbol}: {data}")

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

        DATE_FIELDS_OVERVIEW = {"Ex-Dividend Date", "Earnings Date", "IPO Date"}
        for field in DATE_FIELDS_OVERVIEW:
            if field in data["stats"]:
                data["stats"][field] = self._to_iso_date(data["stats"][field])

        await self._jitter(0.8, 1.5)
        return data

    @retriable(retries=2, delay=30.0)
    async def _scrape_financials(
        self, page: Page, exchange: str, symbol: str, is_etf: bool = False
    ) -> Dict[str, Any]:
        """Scrape the financials table. Returns empty result for ETFs."""
        # Keep the URL exposed in the historical JSON contract. StockAnalysis moved
        # the income statement to a child route, which is used only for navigation.
        url = f"{self._get_base_url(exchange, symbol, is_etf)}/financials/"
        scrape_url = f"{url}income-statement/"

        logger.info(f"Scraping financials for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, scrape_url)
        await self._human_mouse_wander(page)

        try:
            # Current layout: #main-table-main.  Retain #main-table support for
            # any pages still served with the legacy layout.
            await page.wait_for_selector("#main-table-main, #main-table", timeout=20000)
        except Exception:
            await page.wait_for_selector("table", timeout=5000)

        await self._human_scroll(page, passes=5)
        await self._jitter(0.8, 1.25)

        headers = []
        rows = []

        try:
            table = await page.query_selector("#main-table-main, #main-table")
            if not table:
                table = await page.query_selector("table")
            if not table:
                raise RuntimeError("financials table not found")

            header_cells = await page.query_selector_all(
                "#main-table-main thead tr:first-child th, "
                "#main-table thead tr:first-child th"
            )
            if not header_cells:
                header_cells = await table.query_selector_all("thead tr:first-child th")
            headers = [(await c.inner_text()).strip() for c in header_cells]

            data_rows = await table.query_selector_all("tbody tr")
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

    @retriable(retries=2, delay=15.0)
    async def _scrape_dividends(
        self, page: Page, exchange: str, symbol: str, is_etf: bool = False
    ) -> Dict[str, Any]:
        """Scrape the dividend history table."""
        url = f"{self._get_base_url(exchange, symbol, is_etf)}/dividend/"

        logger.info(f"Scraping dividends for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, url)
        await self._human_mouse_wander(page)

        # The markup has two valid forms on StockAnalysis.  Wait for either in
        # one attempt, then retry the whole navigation if neither arrives.  A
        # longer second selector wait only prolonged an anti-bot/error page.
        table = await page.wait_for_selector(".table-wrap table, table", timeout=30000)

        await self._human_scroll(page, passes=4)
        await self._jitter(0.6, 1.2)

        headers = []
        rows = []

        try:
            th_els = await table.query_selector_all("thead th")
            headers = [(await th.inner_text()).strip() for th in th_els]

            tr_els = await table.query_selector_all("tbody tr")
            for tr in tr_els:
                tds = await tr.query_selector_all("td")
                values = [(await td.inner_text()).strip() for td in tds]

                if values:
                    row_dict = {}
                    for i, v in enumerate(values):
                        col = headers[i] if i < len(headers) else f"col_{i}"

                        if col in {"Ex-Dividend Date", "Record Date", "Pay Date"}:
                            v = self._to_iso_date(v)
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
        self, page: Page, exchange: str, symbol: str, is_etf: bool = False
    ) -> Dict[str, Any]:
        """Scrape the statistics page (detailed ratios and metrics)."""
        url = f"{self._get_base_url(exchange, symbol, is_etf)}/statistics/"

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
                    # value = raw.strip() if raw.strip() and raw.strip() != disp else disp

                    if key:
                        section_data[key] = raw.strip() or disp

                if section_data:
                    data["sections"][section_name] = section_data

            DATE_SECTIONS = {"Important Dates", "Stock Splits"}
            DATE_KEYWORDS = {"date", "split date"}

            for section, fields in data["sections"].items():
                if section in DATE_SECTIONS:
                    for key in list(fields.keys()):
                        if any(kw in key.lower() for kw in DATE_KEYWORDS):
                            fields[key] = self._to_iso_date(fields[key])

            # Flat views
            data["all_stats"] = {
                k: v
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
        self, page: Page, exchange: str, symbol: str, is_etf: bool = False
    ) -> Dict[str, Any]:
        """Scrape financial ratios. Returns empty result for ETFs."""
        url = f"{self._get_base_url(exchange, symbol, is_etf)}/financials/ratios/"

        logger.info(f"Scraping ratios for ticker: \t {exchange}:{symbol}")

        await self._safe_goto(page, url)
        await self._human_mouse_wander(page)

        try:
            # Ratios are now split into category tables, each with an ID such as
            # main-table-total-valuation.  The prefix also includes the legacy
            # #main-table ID.
            await page.wait_for_selector("table[id^='main-table']", timeout=20000)
        except Exception:
            await page.wait_for_selector("table", timeout=15000)

        await self._human_scroll(page, passes=5)
        await self._jitter(0.8, 1.25)

        headers = []
        rows = []

        try:
            tables = await page.query_selector_all("table[id^='main-table']")
            if not tables:
                tables = await page.query_selector_all("table")
            if not tables:
                raise RuntimeError("ratios tables not found")

            header_cells = await tables[0].query_selector_all("thead tr:first-child th")
            headers = [(await c.inner_text()).strip() for c in header_cells]

            # Flatten the category tables in document order.  This preserves the
            # historical single headers/rows JSON shape used by the UI and HQL.
            for table in tables:
                data_rows = await table.query_selector_all("tbody tr")
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
        self, context: BrowserContext, ticker: Dict[str, str]
    ) -> Dict[str, Any]:
        exchange = ticker["exchange"]
        symbol = ticker["symbol"]
        key = f"{exchange.upper()}:{symbol}"
        is_etf = self._has_etf_tag(ticker.get("tags"))

        page = await context.new_page()

        result = {
            "ticker": key,
            "scraped_at": dubai_now_iso(),
            "asset_type": "etf" if is_etf else "stock",
        }

        #  Each section is independent — one failure never blocks the others
        sections = [
            ("overview", lambda: self._scrape_overview(page, exchange, symbol, is_etf)),
            (
                "dividends",
                lambda: self._scrape_dividends(page, exchange, symbol, is_etf),
            ),
        ]
        if not is_etf:
            sections.extend(
                [
                    (
                        "financials",
                        lambda: self._scrape_financials(page, exchange, symbol),
                    ),
                    (
                        "statistics",
                        lambda: self._scrape_statistics(page, exchange, symbol),
                    ),
                    ("ratios", lambda: self._scrape_ratios(page, exchange, symbol)),
                ]
            )

        source_blocked = False
        for section_name, scrape_fn in sections:
            try:
                if section_name == "dividends" and self._overview_confirms_no_dividends(
                    result.get("overview")
                ):
                    url = f"{self._get_base_url(exchange, symbol, is_etf)}/dividend/"
                    logger.info(
                        "%s: overview confirms no cash dividends; skipping dividend page",
                        key,
                    )
                    result[section_name] = self._empty_dividends(exchange, symbol, url)
                    continue

                result[section_name] = await scrape_fn()
                if section_name != "ohlc":
                    await self._jitter(1.5, 2.0)
            except Exception as exc:
                logger.warning("%s > %s failed: %s", key, section_name, exc)
                result[section_name] = {"error": str(exc)}
                if isinstance(exc, SourcePageUnavailable) and exc.source_blocked:
                    source_blocked = True
                    break

        # Do not publish a ticker after a source-wide block or an incomplete run.
        if source_blocked:
            result["source_blocked"] = True
            result["error"] = "source challenge or rate limit"
        elif not self.has_usable_result(result):
            result["error"] = "no usable source sections"

        try:
            await page.close()
        except Exception:
            pass

        cooldown = random.uniform(15.0, 30.0)
        logger.info(f"Cooling down {cooldown:.1f}s …")
        await asyncio.sleep(cooldown)

        return result

    # Public API
    async def scrape(self, ticker: dict) -> Dict[str, Any]:
        """
        Scrape tickers and return a dictionary with results. The dictionary has ticker keys (e.g. 'DFM:DEWA') containing the scraped data.
        """

        async with (
            self._virtual_display(not self.headless) as env,
            async_playwright() as pw,
        ):
            context = await self._launch_context(pw, env)
            try:
                return await self._scrape_ticker(context, ticker)
            finally:
                await context.close()
                await asyncio.to_thread(self._clear_browser_cache_if_due)

    async def scrape_dividends_only(self, ticker: dict) -> Dict[str, Any]:
        """Fetch dividends through the same persistent browser profile."""
        exchange = ticker["exchange"]
        symbol = ticker["symbol"]
        is_etf = self._has_etf_tag(ticker.get("tags"))

        async with (
            self._virtual_display(not self.headless) as env,
            async_playwright() as pw,
        ):
            context = await self._launch_context(pw, env)
            try:
                page = await context.new_page()
                try:
                    return await self._scrape_dividends(page, exchange, symbol, is_etf)
                finally:
                    await page.close()
            finally:
                await context.close()
                await asyncio.to_thread(self._clear_browser_cache_if_due)


async def main():
    obj = StockAnalysisScraper()
    res = await obj.scrape({"exchange": "NASDAQ", "symbol": "ZETA"})
    import json

    with open("filename.json", "w") as f:
        json.dump(res, f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
