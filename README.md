# Portfolio Backend

This repository contains the backend for a personal portfolio and market-data
application. It serves portfolio analytics through a FastAPI API and keeps the
underlying market data fresh through a separate scheduled worker.

This document describes the intended production architecture after the Gunicorn
and worker-separation changes are complete. Until that migration is implemented,
some of the current startup scripts and application lifecycle code will still
behave differently.

## What the application does

The application combines several kinds of data:

- Transactions and watchlist entries stored in Google Sheets.
- Intraday and historical OHLC market prices stored in SQLite.
- Company fundamentals scraped from Stock Analysis and stored as JSON files.
- Foreign-exchange rates derived from market prices and stored as JSON.
- Generated quote, watchlist-screening, and holdings-news data stored as JSON.

FastAPI turns this data into portfolio overview, holdings, analytics,
correlation, watchlist, metadata, quote, and system-health responses.

## The production processes

Production runs three distinct layers:

```text
Client
  |
  v
Reverse proxy
  |
  v
Gunicorn master
  |-- API worker 1 (Uvicorn + FastAPI)
  `-- API worker 2 (Uvicorn + FastAPI)

Separate scheduled worker
  |-- APScheduler jobs
  |-- OHLC job runner
  `-- Fundamentals scraper

Shared local data
  |-- cache/portfolio.db
  |-- cache/portfolio.db-wal
  |-- cache/portfolio.db-shm
  `-- cache/*.json
```

The Gunicorn API and the scheduled worker are independent operating-system
services. Starting, stopping, or restarting the API does not start another
scraper. Restarting a Gunicorn API worker also has no effect on scheduled jobs.

### Gunicorn master

Gunicorn owns the listening port and supervises the API worker processes. It
does not execute portfolio logic itself.

The initial production configuration uses two workers:

```bash
gunicorn app.api:app \
  --bind 0.0.0.0:8000 \
  --workers 2 \
  --worker-class uvicorn_worker.UvicornWorker \
  --timeout 60 \
  --graceful-timeout 90 \
  --keep-alive 5 \
  --max-requests 2000 \
  --max-requests-jitter 200
```

`uvicorn_worker.UvicornWorker` lets each process serve the ASGI FastAPI
application. The old `uvicorn.workers.UvicornWorker` import is deprecated and
should not be used for the migration.

Gunicorn sends each incoming connection to one of the two workers. The workers
do not share Python objects or in-memory caches. Each worker has its own FastAPI
application, dependency objects, and memory. They share only external data such
as SQLite, JSON cache files, and Google Sheets.

`max-requests` periodically replaces a worker after it has handled a set number
of requests. The jitter prevents both workers from restarting at the same time.

### Scheduled worker

The scheduled worker runs separately:

```bash
python -m app.worker
```

There is exactly one scheduled worker per server. A process-wide lock prevents
a second copy from running accidentally. The lock is stronger than the existing
`asyncio.Lock`: an `asyncio.Lock` coordinates tasks in one Python process, while
the process-wide lock protects against two separate worker processes.

The worker is the only process allowed to modify the local SQLite database and
the generated JSON cache files. This single-writer rule is the main protection
against local data races.

The API lifecycle must not import and launch `worker_main()`. FastAPI startup is
only responsible for preparing an API worker to serve requests.

## Where data lives

| Data | Source | Local storage | Write owner | Readers |
| --- | --- | --- | --- | --- |
| Transactions | Google Sheets | Included in ticker cache files | Scheduled worker publishes the local copy | API workers |
| Watchlist | Google Sheets | Ticker cache when that ticker has been scraped | API updates Google Sheets; scheduled worker publishes generated local data | API workers and scheduled worker |
| OHLC prices | TradingView data feed | `cache/portfolio.db` | Scheduled worker | API workers |
| Fundamentals | Stock Analysis scraper | `cache/<ticker>.json` | Scheduled worker | API workers |
| FX rates | OHLC market prices | `cache/exchange.json` | Scheduled worker | API workers |
| Quote | Gemini | `cache/quote.json` | Scheduled worker | API workers |
| Watchlist alerts | Google Sheets, fundamentals, and Gemini | `cache/watchlist_alerts.json` | Scheduled worker | API workers |
| Holdings news | Holdings data, news sources, and Gemini | `cache/holdings_news.json` | Scheduled worker | API workers |

The database and all of its WAL files must be on a local filesystem attached to
the same server. SQLite WAL mode must not be used on NFS or another network
filesystem.

## Application startup, end to end

Production startup happens in this order.

### 1. Deploy and prepare the environment

The deployment installs the pinned Python dependencies, including `gunicorn`
and `uvicorn-worker`, and loads the application's environment variables.

Secrets such as Google credentials and Gemini keys stay in environment files or
the service manager. They are not passed in command-line arguments or committed
to the repository.

### 2. Prepare the database

A one-off bootstrap or migration command prepares SQLite before the services
start. It performs schema creation and enables WAL mode:

```sql
PRAGMA journal_mode=WAL;
```

WAL mode persists in the database, so normal API startup does not need to issue
this command repeatedly. Schema creation and migrations also do not run whenever
a request constructs a database object.

The bootstrap verifies that WAL was actually enabled and fails deployment if it
was not.

### 3. Start the scheduled-worker service

The service manager starts `python -m app.worker`. The worker obtains its
single-instance lock, starts APScheduler, and starts the heavy-job runner.

If another worker already holds the lock, the new process exits instead of
running duplicate jobs.

### 4. Start the API service

The service manager starts the Gunicorn master. Gunicorn binds to port 8000 and
creates two Uvicorn worker processes. Each Uvicorn worker imports FastAPI and
builds its own dependency objects.

FastAPI does not start a scraper, scheduler, browser, or background job.

Gunicorn preloading should remain disabled. Import-time side effects and objects
created before a process fork are difficult to reason about, especially for
SQLite and asynchronous libraries.

### 5. Verify readiness

Deployment checks the API through its real HTTP route and performs a simple
read-only database query. It also checks the scheduled-worker heartbeat.

An API health check and a worker health check answer different questions:

- API health confirms that requests can be served.
- Worker health confirms that scheduled data refreshes are still running.

Deployment succeeds only when both are healthy.

## How an API read works

Most requests follow this sequence:

1. A client sends an HTTP request to the reverse proxy.
2. The proxy forwards the request to Gunicorn on port 8000.
3. Gunicorn assigns the connection to one of the two API workers.
4. Uvicorn passes the ASGI request to the matching FastAPI route.
5. The route validates path, query, and request-body values with Pydantic.
6. A service module loads the required Google Sheet, JSON, or SQLite data.
7. The service calculates portfolio results with Python, pandas, and NumPy.
8. FastAPI serializes the result and returns it to the client.

The second request may be handled by a different process. Code must therefore
never rely on a value placed in the memory of API worker 1 being present in API
worker 2.

### Reading SQLite

API database connections are opened in SQLite read-only mode. This makes the
single-writer architecture enforceable rather than merely conventional. An API
bug that attempts an INSERT, UPDATE, DELETE, or schema change fails immediately.

Every database operation follows the same lifecycle:

1. Open a short-lived connection.
2. Configure its row factory and busy timeout.
3. Execute the query and fully fetch its results.
4. Close the connection in a `finally` block, including when the query fails.
5. Convert the detached rows into normal Python dictionaries.

The SQLite connection context manager controls commit and rollback; it does not
close the connection. Explicit closure prevents file descriptors from growing
for the lifetime of an API worker.

The initial busy timeout should be between 5 and 10 seconds. It allows a request
to wait briefly during a checkpoint or short lock instead of immediately
returning `database is locked`. A timeout is protection, not permission for long
write transactions.

### Reading JSON caches

An API worker opens and parses the latest complete JSON file. The scheduled
worker publishes files atomically, so the API sees either the previous complete
version or the next complete version. It never sees half of a JSON document.

If a cache file is missing, the service returns the application's normal empty
or unavailable response and logs the cache miss. It does not attempt to run a
scraper inside the request.

### Synchronous calculations

Many portfolio calculations use synchronous SQLite, pandas, NumPy, filesystem,
or Google client operations. These operations must not run directly for a long
time inside an `async def` route because that blocks the Uvicorn event loop.

Such handlers should either be normal `def` routes, which FastAPI runs in its
thread pool, or explicitly send the blocking calculation to the thread pool.
The event loop remains available for other connections while the calculation is
running.

Two Gunicorn workers provide process-level parallelism, but worker count is not
a substitute for fixing blocking request handlers. Worker count should only be
increased after load testing.

## How API writes work

The API is read-only with respect to local application data. It does not write
SQLite, ticker cache files, quotes, FX files, alert files, or news files.

There is one deliberate external-write path: the watchlist PUT and DELETE
endpoints update Google Sheets. Their sequence is:

1. FastAPI validates the ticker and payload.
2. The route calls the Google Sheets manager.
3. Google Sheets updates or deletes the watchlist row.
4. The API returns success or a clear validation/not-found error.
5. The scheduled worker sees the new sheet contents during its next relevant
   cycle and refreshes local generated data when needed.

These endpoints should have authentication and request limits because running
multiple API workers makes more simultaneous Google API calls possible.

## How scraping and local writes work

All local writes flow through the one scheduled-worker process.

### Heavy-job runner

The continuous heavy-job runner decides what to do based on Dubai time and the
configured exchange sessions.

During an open market window:

1. Load portfolio transactions and watchlist entries from Google Sheets.
2. Add configured benchmark instruments.
3. Remove duplicate ticker entries.
4. Skip exchanges that are outside their configured market window.
5. Fetch OHLC bars for each eligible instrument.
6. Write those bars to SQLite in batches.
7. Run one fundamentals drip scrape.
8. Sleep until the next 15-minute cycle, accounting for time already spent.

Outside market hours:

1. Check whether a fundamentals ticker is due.
2. Scrape exactly one due ticker when work exists.
3. Sleep briefly after success, longer after failure, or much longer when the
   week's work is complete.

The in-process job lock keeps OHLC and fundamentals work from overlapping. The
process-wide lock guarantees that only one copy of this entire runner exists.

### OHLC write sequence

An OHLC refresh works as follows:

1. Fetch bars from the TradingView data source outside the API processes.
2. Normalize timestamps to the application's Dubai-time representation.
3. Build one collection of rows for the ticker.
4. Open a writable SQLite connection with a busy timeout.
5. Begin a short transaction.
6. Upsert all rows with `executemany`.
7. Commit the transaction.
8. Roll back on error and always close the connection.

The primary key of symbol plus timestamp makes repeated scrapes idempotent. A
repeat updates the existing bar rather than creating a duplicate.

### Fundamentals write sequence

The fundamentals drip job builds a queue from transactions and watchlist
entries. Its priority is:

1. Tickers that have never been scraped.
2. Tickers whose purchase details changed.
3. Tickers not yet refreshed during the current ISO week.

Only one ticker is selected for each drip run. The scraper has a bounded timeout
and tracks consecutive failures. Repeated failures place a ticker into cooldown
so one broken external page cannot block the entire queue.

After a successful scrape, the worker adds the current purchase details and
publishes the ticker cache atomically.

### Lightweight scheduled jobs

APScheduler runs the lighter cron-style tasks:

- FX refresh on weekday mornings.
- Quote generation on the configured weekly schedule.
- Watchlist AI screening each day at the configured time.
- Holdings-news checks each day at the configured time.

`max_instances=1` prevents two copies of the same scheduled job from overlapping
inside the worker. It does not replace the process-wide singleton lock.

### Atomic JSON publication

Every generated JSON write uses this publication model:

1. Serialize the complete payload.
2. Create a temporary file in the same cache directory.
3. Write the complete contents.
4. Flush the file and close it.
5. Atomically replace the destination with `os.replace()`.

Using the same directory is important because it keeps the rename on the same
filesystem. Readers continue to use the old complete file until the replacement
becomes visible in one operation.

Where one job performs a read-modify-write operation, that complete operation
stays inside the single scheduled worker so updates cannot overwrite each other.

## How SQLite WAL works here

In the default SQLite journal mode, a writer can block readers more aggressively.
WAL changes where new writes are recorded:

- `portfolio.db` is the main database.
- `portfolio.db-wal` temporarily contains committed changes not yet merged into
  the main file.
- `portfolio.db-shm` coordinates local processes reading the WAL.

API workers read a stable snapshot while the scheduled worker appends a short
transaction to the WAL. This is why readers and the writer can normally proceed
at the same time.

SQLite still permits only one writer at a time. The application makes that
constraint explicit by assigning all local writes to the scheduled worker.

SQLite periodically checkpoints WAL contents back into the main database. The
default automatic checkpoint is suitable initially. Production monitoring
should alert if the WAL file grows continuously, because that can indicate a
long-running reader or a checkpoint problem.

The database, WAL, and shared-memory files form one working set. Backups must use
SQLite's backup mechanism or stop all application access before copying the set.
Copying only `portfolio.db` while writes are active can omit recent committed
data.

## Service management and deployment

Production should use two systemd units:

- `pbe-api.service` for Gunicorn.
- `pbe-worker.service` for `python -m app.worker`.

The service manager provides process ownership, environment loading, log
collection, automatic restart, signal delivery, and a reliable process ID.
Deployment scripts should not use `pkill`, `nohup`, `disown`, or broad Chromium
process kills.

A normal deployment is sequential:

1. Fetch or copy the new application version.
2. Install its pinned dependencies.
3. Run automated tests.
4. Back up SQLite safely before a schema migration.
5. Run the one-off database migration/bootstrap.
6. Restart the scheduled worker gracefully.
7. Gracefully reload Gunicorn so workers are replaced without dropping all
   traffic at once.
8. Check API readiness.
9. Check the worker heartbeat and recent job status.
10. Mark the deployment successful or roll back.

Gunicorn should receive `SIGTERM` for shutdown or `SIGHUP` for a graceful worker
reload. The scheduled worker should stop accepting new jobs, cancel or finish
the active job according to its shutdown policy, close Playwright, shut down
APScheduler, release the singleton lock, and exit.

The worker's scrape timeout is currently much longer than the API's graceful
timeout. Its systemd stop timeout must account for that, or shutdown must
explicitly cancel Playwright and record the interrupted job as retryable.

## Failure behavior

The architecture is designed so a failure stays within its own boundary.

### One API worker crashes

Gunicorn continues serving with the remaining worker and starts a replacement.
The scheduled worker is unaffected.

### The scheduled worker crashes

The API continues serving the latest completed local data. Systemd restarts the
worker. Monitoring reports that freshness is falling behind.

### An external scraper fails

The job logs the error, retries according to its policy, and eventually places a
repeatedly failing ticker into cooldown. Existing completed cache data remains
available.

### SQLite is briefly busy

The connection waits for the configured busy timeout. If the timeout expires,
the operation fails with a clear database error and is logged with the operation
and duration. The code does not retry forever.

### A JSON generation job fails

The temporary file is discarded and the previous complete destination remains
available. The failed payload never replaces working data.

### Google Sheets is unavailable

Requests that require live Google data return a controlled upstream-service
error. Requests backed entirely by local cache data can continue working.
Scheduled jobs retain existing data and try again on their next run.

## Observability

API and worker logs should be separate and collected by systemd's journal or the
chosen log platform.

API logs should include:

- Request method, route, status, duration, and worker process ID.
- Unhandled exceptions without secret values.
- SQLite busy-timeout and JSON-parse failures.
- Worker starts, graceful reloads, and forced terminations.

Scheduled-worker logs should include:

- Worker process ID and singleton-lock acquisition.
- Job name, start time, end time, duration, and outcome.
- Ticker counts, refreshed counts, skips, retries, and cooldowns.
- Last successful OHLC, fundamentals, FX, quote, screening, and news times.
- Worker shutdown and Playwright cleanup.

Useful production measurements include:

- API request rate, error rate, and p50/p95/p99 latency.
- Gunicorn worker memory and restarts.
- Open file-descriptor count per process.
- SQLite busy errors and WAL-file size.
- Cache age by data type.
- Scheduled-job duration and consecutive failures.

## Validation before enabling two API workers

The migration is ready when all of these checks pass:

- Repeated database reads and writes do not increase open file descriptors.
- Two independent reader processes can query while the scheduled worker writes.
- Concurrency tests produce no unexpected `database is locked` errors.
- API processes cannot modify SQLite or generated cache files.
- Repeated JSON reads never observe an incomplete document during publication.
- Starting two scheduled workers causes the second one to fail its singleton
  lock.
- Starting two Gunicorn workers does not create APScheduler or Playwright tasks.
- Graceful API reloads complete without a full outage.
- Worker shutdown closes browser processes and leaves jobs safely retryable.
- Load testing shows acceptable latency and memory use with two workers.
- A tested rollback can return the API to one Gunicorn worker without reconnecting
  scraping to the FastAPI lifecycle.
