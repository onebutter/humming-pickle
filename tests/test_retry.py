"""Retry behavior: a job that hits transient 5xx is retried with exponential
backoff up to MAX_ATTEMPTS, then transitions to `failed`.

We can't deterministically force the random-5xx provider to fail, so instead
we submit enough jobs that — at the configured 20% failure rate — at least
one will retry and be observable via `attempts > 1`.
"""

import asyncio
import time

import pytest


async def _submit(http, key, email):
    r = await http.post(
        "/verifications",
        headers={"X-API-Key": key},
        json={"subject_email": email, "metadata": {}},
    )
    r.raise_for_status()
    return r.json()["id"]


@pytest.mark.asyncio
async def test_some_jobs_retry_under_flaky_provider(http, org_a_key):
    ids = [await _submit(http, org_a_key, f"retry-{i}@a.test") for i in range(30)]

    deadline = time.monotonic() + 180
    completed = []
    while time.monotonic() < deadline:
        completed.clear()
        for jid in ids:
            r = await http.get(
                f"/verifications/{jid}", headers={"X-API-Key": org_a_key}
            )
            j = r.json()
            if j["status"] in ("completed", "failed"):
                completed.append(j)
        if len(completed) == len(ids):
            break
        await asyncio.sleep(1.0)

    assert len(completed) == len(ids), "not all jobs reached terminal state"
    max_attempts = max(j["attempts"] for j in completed)
    assert max_attempts >= 2, (
        f"no job retried — max attempts seen was {max_attempts}. "
        f"Provider's failure rate may be misconfigured."
    )
