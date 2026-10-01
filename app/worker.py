# app/worker.py

import asyncio
import signal
import time
from collections import defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import app.config  # noqa

from app.config import WORKER_LOCK_PATH
from app.core.singleton import SingletonLock
from app.utils.time_utils import DUBAI_TZ, dubai_now
from app.core.logger import get_logger
from app.scraper.ohlc import _set_ohlc
from app.scraper.sa import StockAnalysisScraper
from app.data.gsheet import GSheet_Manager
from app.data.cache import Cache
from app.data.rescrape_queue import RescrapeQueue, RescrapeRequest
from app.data.fx import fetch_and_save_fx
from app.services.quote import QuoteStore
from app.services.watchlist import WatchlistModule
from app.services.watchlist_ai import WatchlistAIScreener
from app.data.db import DB
from app.services.holdings_news import HoldingsNewsAgent

from datetime import date, datetime, timedelta

logger = get_logger()

# Double-guard lock: job_runner is the primary sequencer,
# but this lock protects against any direct manual calls too.
_job_lock = asyncio.Lock()

_scrape_failures: dict[str, int] = defaultdict(int)
_FAILURE_COOLDOWN_SECS = 6 * 60 * 60  # 6 hours after 3 consecutive failures
_FAILURE_THRESHOLD = 3
_scrape_cooldown_until: dict[str, float] = {}
_next_fundamentals_scrape_at = 0.0
_source_cooldown_until = 0.0


def _fundamentals_seconds_until_next_scrape() -> float:
    """Return remaining process-wide source cooldown, never below zero."""
    now = time.monotonic()
    return max(
        0.0,
        _next_fundamentals_scrape_at - now,
        _source_cooldown_until - now,
    )


def _reserve_fundamentals_scrape_slot() -> float:
    """Reserve the next SA slot and return its minimum start-to-start spacing."""
    global _next_fundamentals_scrape_at
    interval = max(0, app.config.FUNDAMENTALS_MIN_INTERVAL_SECONDS)
    _next_fundamentals_scrape_at = time.monotonic() + interval
    return interval


def _open_source_circuit() -> float:
    """Pause all SA work when the site signals a rate limit or challenge."""
    global _source_cooldown_until
    cooldown = max(0, app.config.FUNDAMENTALS_SOURCE_COOLDOWN_SECONDS)
    _source_cooldown_until = time.monotonic() + cooldown
    return cooldown


def _quarantine_source_blocked_ticker(ticker: str) -> float:
    """Prevent one blocked ticker from pinning the priority queue.

    A 403/429 is normally source-wide, so opening the source circuit is the
    primary response. The same ticker must also be skipped on the next source
    attempt; otherwise a first-priority new/updated ticker is selected again
    indefinitely whenever the source circuit expires.
    """
    _scrape_failures[ticker] += 1
    until = time.time() + _FAILURE_COOLDOWN_SECS
    _scrape_cooldown_until[ticker] = until
    return until


def _current_week_key() -> tuple[int, int]:
    iso = dubai_now().date().isocalendar()
    return iso.year, iso.week


def _week_key_from_scraped_at(scraped_at: str | None) -> tuple[int, int] | None:
    if not scraped_at:
        return None
    try:
        d = date.fromisoformat(scraped_at[:10])
        iso = d.isocalendar()
        return iso.year, iso.week
    except Exception:
        return None


def _was_scraped_this_week(cached_data: dict | None) -> bool:
    # A timestamp alone is not freshness.  Older partial runs were published
    # despite empty/challenge sections, which left tickers such as MCD skipped
    # for the rest of the week.  Keep those tickers due until a usable result
    # has actually been collected.
    if not cached_data or not StockAnalysisScraper.has_usable_result(cached_data):
        return False
    return (
        _week_key_from_scraped_at(cached_data.get("scraped_at")) == _current_week_key()
    )


def _session_time(session_date: date, value: object) -> datetime:
    """Build a Dubai datetime from a configurable ``HH:MM`` session value."""
    hour, minute = map(int, str(value).split(":", 1))
    return datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        hour,
        minute,
        tzinfo=DUBAI_TZ,
    )


def _is_ohlc_exchange_open(exchange: object, now: datetime | None = None) -> bool:
    """Whether an exchange is within its configured UAE OHLC refresh window."""
    current = (now or dubai_now()).astimezone(DUBAI_TZ)

    # OHLC refreshes are never run on Saturday or Sunday, including any apparent
    # overnight buffer after Friday's US session.
    if current.weekday() >= 5:
        return False

    exchange_key = str(exchange or "").strip().upper()
    session = app.config.OHLC_MARKET_SESSIONS.get(
        exchange_key, app.config.OHLC_MARKET_SESSIONS["DEFAULT"]
    )
    session_weekdays = set(session["weekdays"])
    buffer = timedelta(minutes=app.config.OHLC_SESSION_BUFFER_MINUTES)

    # Check today's session and the preceding one.  The latter is needed for US
    # sessions whose 00:00 close and post-close buffer cross into the next day.
    for session_date in (current.date(), current.date() - timedelta(days=1)):
        if session_date.weekday() not in session_weekdays:
            continue

        opens_at = _session_time(session_date, session["open"])
        closes_at = _session_time(session_date, session["close"])
        if closes_at <= opens_at:
            closes_at += timedelta(days=1)

        if opens_at - buffer <= current <= closes_at + buffer:
            return True

    return False


def _is_any_ohlc_market_open(now: datetime | None = None) -> bool:
    """Return whether at least one configured OHLC session is currently active."""
    return any(
        _is_ohlc_exchange_open(exchange, now)
        for exchange in app.config.OHLC_MARKET_SESSIONS
    )


# Lightweight jobs — keep their cron schedules, no Playwright involved


async def fx_job():
    logger.info("FX job starting")
    try:
        await fetch_and_save_fx()
    except Exception:
        logger.exception("FX job failed")


async def quote_job():
    store = QuoteStore()
    store.write()


async def watchlist_screening_job():
    logger.info("Watchlist screening job starting")
    try:
        gs = GSheet_Manager()
        raw_items = gs.fetch_watchlist()

        module = WatchlistModule(Cache(), DB())
        enriched = module.get_watchlist(raw_items)

        stored = WatchlistAIScreener.read()
        stored_by_ticker = {a["ticker"]: a for a in stored.get("alerts", [])}
        today = date.today()

        def is_due(item: dict) -> bool:
            ticker = item["ticker"]
            stored_alert = stored_by_ticker.get(ticker)
            if not stored_alert:
                return True
            screened_at = stored_alert.get("screened_at")
            if screened_at:
                try:
                    age = (today - date.fromisoformat(screened_at[:10])).days
                    if age >= 14:
                        return True
                except (ValueError, TypeError):
                    return True
            next_check = stored_alert.get("next_check_date")
            if not next_check:
                return True
            try:
                return today >= date.fromisoformat(next_check)
            except (ValueError, TypeError):
                return True

        due_items = [i for i in enriched if is_due(i)]
        logger.info("%d/%d tickers due for screening", len(due_items), len(enriched))

        if not due_items:
            logger.info("No tickers due today, skipping screening")
            return

        fundamentals_map = {
            item["ticker"]: (
                (data.statistics.dict() if data.statistics else {})
                if (data := module.get_ticker(item["ticker"]))
                else {}
            )
            for item in due_items
        }

        new_alerts = WatchlistAIScreener().run(due_items, fundamentals_map)

        updated = {a["ticker"]: a for a in stored.get("alerts", [])}
        for alert in new_alerts:
            updated[alert["ticker"]] = alert

        screener = WatchlistAIScreener()
        screener._persist(list(updated.values()))

    except Exception:
        logger.exception("Watchlist screening job failed")


# Heavy jobs — orchestrated exclusively by job_runner, never by cron


async def ohlc_job(bars: int = 50):
    async with _job_lock:
        logger.info("OHLC job starting | now=%s", dubai_now().isoformat())
        try:
            gs = GSheet_Manager()
            tickers = gs.fetch_transactions() + gs.fetch_watchlist()

            benchmark_list = [
                {"ticker": k, **v} for k, v in app.config.BENCHMARKS.items()
            ]
            tickers += benchmark_list

            seen = set()
            refreshed = 0
            skipped_by_exchange: dict[str, int] = defaultdict(int)
            for t in tickers:
                key = t.get("ticker")
                if not key or key in seen:
                    continue
                seen.add(key)

                exchange = t.get("exchange")
                if not _is_ohlc_exchange_open(exchange):
                    skipped_by_exchange[str(exchange or "UNKNOWN").upper()] += 1
                    continue

                try:
                    await _set_ohlc(
                        tv_exchange=exchange,
                        symbol=t["symbol"],
                        bars=bars,
                    )
                    refreshed += 1
                except Exception:
                    logger.exception("OHLC failed for %s", key)

            logger.info(
                "OHLC job complete | refreshed=%d skipped_closed=%s",
                refreshed,
                dict(skipped_by_exchange),
            )
        except Exception:
            logger.exception("OHLC job failed")


async def fundamentals_drip_job() -> str:
    """
    Scrapes EXACTLY ONE ticker per run.

    Returns:
        "scraped" -> a ticker was successfully scraped
        "failed"  -> a ticker was attempted but failed/timed out
        "idle"         -> nothing is due this week
        "rate_limited" -> a previous source attempt is still being paced
        "rescrape_rate_limited" -> a manual request is waiting for that pace
    """
    async with _job_lock:
        logger.info("Fundamentals drip starting | now=%s", dubai_now().isoformat())
        try:
            rescrape_queue = RescrapeQueue()
            requested_rescrape = rescrape_queue.next()
            gs = GSheet_Manager()
            transactions = gs.fetch_transactions()
            watchlist = gs.fetch_watchlist()

            purchases_map: dict[str, list] = defaultdict(list)
            for txn in transactions:
                if key := txn.get("ticker"):
                    purchases_map[key].append(txn)

            all_info: dict[str, dict] = {}
            for txn in transactions:
                if key := txn.get("ticker"):
                    all_info[key] = txn
            for item in watchlist:
                if key := item.get("ticker"):
                    if key not in all_info:
                        all_info[key] = item
                    elif item.get("tags"):
                        # A watchlist classification (for example, ETF) also
                        # applies when the ticker appears in transactions.
                        all_info[key]["tags"] = item["tags"]

            if not all_info:
                logger.info("No tickers found in sheets — nothing to scrape.")
                return "idle"

            cache = Cache(read_only=False)

            missing_tickers: list[str] = []
            updated_tickers: list[str] = []
            due_this_week_tickers: list[tuple[str, str]] = []
            fresh_this_week = 0

            for key in all_info:
                # Skip tickers currently in failure cooldown
                if time.time() < _scrape_cooldown_until.get(key, 0):
                    logger.debug("Skipping %s — in failure cooldown", key)
                    fresh_this_week += 1  # count it as "not due" so the log stays clean
                    continue

                cached_data = cache.load(key)

                # Priority 1: never scraped
                if not cached_data:
                    missing_tickers.append(key)
                    continue

                # Priority 2: purchase details changed since last scrape
                if cached_data.get("purchase_details") != purchases_map[key]:
                    updated_tickers.append(key)
                    continue

                # Weekly cap: if already scraped this ISO week, skip it
                if _was_scraped_this_week(cached_data):
                    fresh_this_week += 1
                    continue

                # Priority 3: due again because it has NOT been scraped this week
                scraped_at = cached_data.get("scraped_at", "1970-01-01T00:00:00")
                due_this_week_tickers.append((key, scraped_at))

            logger.info(
                "Fundamentals queue | new=%d updated=%d due=%d already_done_this_week=%d total=%d",
                len(missing_tickers),
                len(updated_tickers),
                len(due_this_week_tickers),
                fresh_this_week,
                len(all_info),
            )

            target_key: str | None = None

            manual_rescrape: RescrapeRequest | None = None
            if requested_rescrape:
                if requested_rescrape.ticker not in all_info:
                    # The instrument was removed from both sheets after the UI
                    # queued it, so it can never be resolved by this worker.
                    logger.warning(
                        "Discarding rescrape request for unknown ticker: %s",
                        requested_rescrape.ticker,
                    )
                    rescrape_queue.discard(requested_rescrape)
                    return "idle"
                target_key = requested_rescrape.ticker
                manual_rescrape = requested_rescrape
                logger.info("Priority 0 — manual rescrape: %s", target_key)
            elif missing_tickers:
                target_key = missing_tickers[0]
                logger.info("Priority 1 — new ticker: %s", target_key)
            elif updated_tickers:
                target_key = updated_tickers[0]
                logger.info("Priority 2 — updated transactions: %s", target_key)
            elif due_this_week_tickers:
                due_this_week_tickers.sort(key=lambda x: x[1])
                target_key, last_scraped = due_this_week_tickers[0]
                logger.info(
                    "Priority 3 — due this week: %s (last scraped: %s)",
                    target_key,
                    last_scraped,
                )
            else:
                logger.info("No fundamentals work due this week.")
                return "idle"

            remaining = _fundamentals_seconds_until_next_scrape()
            if remaining:
                logger.info(
                    "Fundamentals source pacing | next attempt in %.0fs", remaining
                )
                return "rescrape_rate_limited" if requested_rescrape else "rate_limited"

            info = all_info[target_key]
            obj = StockAnalysisScraper()
            interval = _reserve_fundamentals_scrape_slot()
            logger.info(
                "Fundamentals source slot reserved | ticker=%s min_interval=%.0fs",
                target_key,
                interval,
            )

            try:
                scrape = await asyncio.wait_for(
                    obj.scrape(
                        {
                            "exchange": info["sa_exchange"],
                            "symbol": info["sa_symbol"],
                            "tags": info.get("tags"),
                        }
                    ),
                    timeout=480,
                )
                if scrape.get("source_blocked"):
                    cooldown = _open_source_circuit()
                    ticker_cooldown_until = _quarantine_source_blocked_ticker(
                        target_key
                    )
                    logger.error(
                        "StockAnalysis blocked %s; source circuit open for %.0fs "
                        "and ticker quarantined for %.0fh",
                        target_key,
                        cooldown,
                        max(0, ticker_cooldown_until - time.time()) / 3600,
                    )
                    if manual_rescrape:
                        rescrape_queue.discard(manual_rescrape)
                    return "rate_limited"
                if not obj.has_usable_result(scrape):
                    raise RuntimeError("scrape returned no usable source sections")
                scrape["purchase_details"] = purchases_map[target_key]
                cache.save(target_key, scrape)
                logger.info("Fundamentals saved: %s", target_key)

                # Reset failure tracking on success
                _scrape_failures.pop(target_key, None)
                _scrape_cooldown_until.pop(target_key, None)
                if manual_rescrape:
                    rescrape_queue.discard(manual_rescrape)
                return "scraped"

            except asyncio.TimeoutError:
                logger.error(
                    "Fundamentals timed out for %s — patching purchase_details and moving on",
                    target_key,
                )

                _scrape_failures[target_key] += 1
                if _scrape_failures[target_key] >= _FAILURE_THRESHOLD:
                    until = time.time() + _FAILURE_COOLDOWN_SECS
                    _scrape_cooldown_until[target_key] = until
                    logger.error(
                        "%s failed %d times — cooling down for %.0fh",
                        target_key,
                        _scrape_failures[target_key],
                        _FAILURE_COOLDOWN_SECS / 3600,
                    )

                # Patch purchase_details into whatever is cached so this ticker
                # stops appearing as Priority 2 on the next cycle.
                existing = cache.load(target_key) or {}
                existing["purchase_details"] = purchases_map[target_key]
                cache.save(target_key, existing)
                if manual_rescrape:
                    rescrape_queue.discard(manual_rescrape)
                return "failed"

            except Exception:
                logger.exception("Fundamentals failed for %s", target_key)
                existing = cache.load(target_key) or {}

                _scrape_failures[target_key] += 1
                if _scrape_failures[target_key] >= _FAILURE_THRESHOLD:
                    until = time.time() + _FAILURE_COOLDOWN_SECS
                    _scrape_cooldown_until[target_key] = until
                    logger.error(
                        "%s failed %d times — cooling down for %.0fh",
                        target_key,
                        _scrape_failures[target_key],
                        _FAILURE_COOLDOWN_SECS / 3600,
                    )

                existing["purchase_details"] = purchases_map[target_key]
                cache.save(target_key, existing)
                if manual_rescrape:
                    rescrape_queue.discard(manual_rescrape)
                return "failed"

        except Exception:
            logger.exception("Fundamentals drip job failed")
            return "failed"


def run_holdings_news_check():
    HoldingsNewsAgent().run()


# Job Runner — single continuous loop, the ONLY place OHLC + drip are called


async def _sleep_or_stop(
    stop_event: asyncio.Event, seconds: float, *, wake_for_rescrape: bool = True
) -> bool:
    """Sleep until the next cycle, waking promptly for a manual rescrape."""
    deadline = time.monotonic() + seconds
    while not stop_event.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=min(30, remaining))
            return True
        except asyncio.TimeoutError:
            # The filesystem queue is the cross-process signal from API
            # workers.  Polling it here lets an idle worker react within 30s
            # without ever launching another worker or scraper in the API.
            if wake_for_rescrape and RescrapeQueue().has_pending():
                return False
    return True


async def job_runner(stop_event: asyncio.Event):
    """
    Serial orchestrator for heavy jobs only.

    Any configured market window (including its buffer):
      OHLC -> one fundamentals drip -> sleep until next 15-min cycle.

    Off-hours:
      Run drip only when there is work due.
      If everything for the current ISO week is already done, sleep longer.
    """
    logger.info("Job runner started")

    while not stop_event.is_set():
        try:
            cycle_start = dubai_now()
            weekday = cycle_start.weekday()  # 0=Mon ... 6=Sun
            is_ohlc_window = _is_any_ohlc_market_open(cycle_start)

            if is_ohlc_window:
                logger.info("Job runner: active OHLC market window — running OHLC")
                await ohlc_job()

                logger.info(
                    "Job runner: active OHLC market window — running one drip scrape"
                )
                drip_status = await fundamentals_drip_job()

                elapsed = (dubai_now() - cycle_start).total_seconds()
                sleep_for = max(30, (15 * 60) - elapsed)

                logger.info(
                    "Job runner: market cycle done | drip_status=%s | elapsed=%.0fs | sleep=%.0fs",
                    drip_status,
                    elapsed,
                    sleep_for,
                )
                if await _sleep_or_stop(
                    stop_event,
                    sleep_for,
                    wake_for_rescrape=drip_status != "rescrape_rate_limited",
                ):
                    break

            else:
                logger.info("Job runner: off-hours — checking fundamentals work")
                drip_status = await fundamentals_drip_job()

                if drip_status in {
                    "scraped",
                    "rate_limited",
                    "rescrape_rate_limited",
                }:
                    sleep_for = max(30, _fundamentals_seconds_until_next_scrape())
                elif drip_status == "failed":
                    sleep_for = 15 * 60
                else:
                    # idle: nothing due this week, so chill
                    sleep_for = 120 * 60 if weekday >= 5 else 60 * 60

                logger.info(
                    "Job runner: off-hours | drip_status=%s | sleep=%.0fs",
                    drip_status,
                    sleep_for,
                )
                if await _sleep_or_stop(
                    stop_event,
                    sleep_for,
                    wake_for_rescrape=drip_status != "rescrape_rate_limited",
                ):
                    break

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Job runner error — resuming in 60s")
            if await _sleep_or_stop(stop_event, 60):
                break


# Entry point


async def main():
    lock = SingletonLock(WORKER_LOCK_PATH)
    lock.acquire()

    scheduler = AsyncIOScheduler(timezone=DUBAI_TZ)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        logger.info("Worker shutdown requested")
        stop_event.set()

    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(shutdown_signal, request_stop)
        except NotImplementedError:
            # The production service runs on Linux. This keeps direct local
            # execution usable on platforms without asyncio signal handlers.
            pass

    # Lightweight cron jobs — these never touch Playwright
    scheduler.add_job(
        fx_job,
        "cron",
        day_of_week="mon-fri",
        hour=6,
        minute=0,
        id="fx_daily",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        quote_job,
        "cron",
        day_of_week="mon",
        hour=0,
        minute=0,
        id="quote_daily",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        watchlist_screening_job,
        "cron",
        hour=18,
        minute=0,
        timezone="Asia/Dubai",
        id="watchlist_screening",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        run_holdings_news_check,
        "cron",
        hour=12,
        minute=0,
        id="holdings_news",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.start()

    # Start the heavy job runner as a background task.
    # OHLC and fundamentals scraping are managed solely here.
    runner_task = asyncio.create_task(job_runner(stop_event), name="job-runner")

    # Manual one-shot triggers:
    # await ohlc_job(bars=100)
    # await fundamentals_drip_job()
    # await fx_job()
    # await quote_job()
    # await watchlist_screening_job()
    # run_holdings_news_check()

    try:
        await stop_event.wait()
    finally:
        scheduler.shutdown(wait=False)
        runner_task.cancel()
        await asyncio.gather(runner_task, return_exceptions=True)
        lock.release()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
