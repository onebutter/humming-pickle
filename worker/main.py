"""Verification worker.

Two asyncio loops, one process:

  dispatcher  — claims the next eligible job (fairness query), acquires a
                token from the global bucket, fires a background coroutine
                to call the enrichment provider, handles retries/terminal
                state. Throughput is bounded by the bucket (10 r/s).

  reclaimer   — every second, returns orphaned `in_progress` rows back to
                pending (visibility-timeout pattern). This is the crash-
                recovery story: workers can die mid-job and no work is lost.

Connects as `app_worker`, which has BYPASSRLS — the dispatcher needs cross-
org visibility to schedule fairly.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from bucket import TokenBucket
from queries import (
    BUMP_ORG_SERVED_AT,
    CLAIM_NEXT_JOB,
    COMPLETE_JOB,
    FAIL_JOB,
    RECLAIM_STUCK,
    RETRY_JOB,
)

DATABASE_URL = os.environ["DATABASE_URL"]
ENRICHMENT_URL = os.environ["ENRICHMENT_URL"]
RATE_LIMIT_PER_SEC = float(os.environ.get("RATE_LIMIT_PER_SEC", "10"))
RATE_LIMIT_BURST = float(os.environ.get("RATE_LIMIT_BURST", "10"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
RECLAIM_AFTER_SECONDS = int(os.environ.get("RECLAIM_AFTER_SECONDS", "60"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}',
)
# Library loggers don't emit JSON; quiet them so the worker log is clean.
for noisy in ("httpx", "httpcore", "sqlalchemy"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger("worker")


def _evt(event: str, **fields: Any) -> str:
    return json.dumps({"event": event, **fields})


engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, pool_size=20)
Session = async_sessionmaker(engine, expire_on_commit=False)

bucket = TokenBucket(rate=RATE_LIMIT_PER_SEC, capacity=RATE_LIMIT_BURST)


def _backoff_seconds(attempts: int) -> int:
    # Exponential: 1, 2, 4, 8, 16 ... seconds.
    return 2 ** (attempts - 1)


async def _process_job(job: dict[str, Any], client: httpx.AsyncClient) -> None:
    """Call enrichment, write the outcome. Runs as a fire-and-forget task."""
    job_id = job["id"]
    org_id = job["org_id"]
    attempts = job["attempts"]

    try:
        resp = await client.post(
            f"{ENRICHMENT_URL}/enrich",
            json={
                "subject_email": job["subject_email"],
                "metadata": job["metadata"],
            },
            timeout=30.0,
        )
        if resp.status_code == 200:
            async with Session() as s, s.begin():
                await s.execute(
                    text(COMPLETE_JOB),
                    {"id": str(job_id), "result": json.dumps(resp.json())},
                )
            log.info(_evt("job_completed", job_id=str(job_id),
                          org_id=str(org_id), attempts=attempts))
            return
        # Treat 5xx and 429 as transient; other 4xx are terminal.
        transient = resp.status_code >= 500 or resp.status_code == 429
        error = {"status": resp.status_code, "body": resp.text[:200]}
        await _handle_failure(job_id, org_id, attempts, error, transient)
    except (httpx.RequestError, asyncio.TimeoutError) as e:
        error = {"error": type(e).__name__, "detail": str(e)[:200]}
        await _handle_failure(job_id, org_id, attempts, error, transient=True)


async def _handle_failure(
    job_id: Any, org_id: Any, attempts: int, error: dict, transient: bool
) -> None:
    if not transient or attempts >= MAX_ATTEMPTS:
        async with Session() as s, s.begin():
            await s.execute(
                text(FAIL_JOB),
                {"id": str(job_id), "last_error": json.dumps(error)},
            )
        log.warning(_evt("job_failed", job_id=str(job_id),
                         org_id=str(org_id), attempts=attempts, error=error))
        return
    backoff = _backoff_seconds(attempts)
    async with Session() as s, s.begin():
        await s.execute(
            text(RETRY_JOB),
            {
                "id": str(job_id),
                "backoff_seconds": str(backoff),
                "last_error": json.dumps(error),
            },
        )
    log.info(_evt("job_retry_scheduled", job_id=str(job_id),
                  org_id=str(org_id), attempts=attempts,
                  backoff_seconds=backoff, error=error))


async def _claim_next() -> dict[str, Any] | None:
    async with Session() as s, s.begin():
        row = (await s.execute(text(CLAIM_NEXT_JOB))).first()
        if row is None:
            return None
        await s.execute(text(BUMP_ORG_SERVED_AT), {"org_id": str(row.org_id)})
        return {
            "id": row.id,
            "org_id": row.org_id,
            "subject_email": row.subject_email,
            "metadata": row.metadata,
            "attempts": row.attempts,
        }


async def dispatcher_loop(stop: asyncio.Event) -> None:
    log.info(_evt("dispatcher_started",
                  rate_per_sec=RATE_LIMIT_PER_SEC, burst=RATE_LIMIT_BURST))
    async with httpx.AsyncClient() as client:
        while not stop.is_set():
            job = await _claim_next()
            if job is None:
                # Idle backoff. Short enough that submitted work is picked up
                # responsively; long enough not to hammer the DB at idle.
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.2)
                except asyncio.TimeoutError:
                    pass
                continue
            await bucket.acquire()
            log.info(_evt("job_dispatched", job_id=str(job["id"]),
                          org_id=str(job["org_id"]), attempts=job["attempts"]))
            asyncio.create_task(_process_job(job, client))
    log.info(_evt("dispatcher_stopped"))


async def reclaimer_loop(stop: asyncio.Event) -> None:
    log.info(_evt("reclaimer_started",
                  threshold_seconds=RECLAIM_AFTER_SECONDS))
    while not stop.is_set():
        try:
            async with Session() as s, s.begin():
                result = await s.execute(
                    text(RECLAIM_STUCK),
                    {"threshold_seconds": str(RECLAIM_AFTER_SECONDS)},
                )
                rows = result.fetchall()
                if rows:
                    log.warning(_evt("jobs_reclaimed",
                                     count=len(rows),
                                     ids=[str(r.id) for r in rows]))
        except Exception as e:
            log.error(_evt("reclaimer_error", error=str(e)))
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass
    log.info(_evt("reclaimer_stopped"))


async def main() -> None:
    stop = asyncio.Event()

    def _signal_handler(*_: Any) -> None:
        log.info(_evt("signal_received"))
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await asyncio.gather(dispatcher_loop(stop), reclaimer_loop(stop))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
