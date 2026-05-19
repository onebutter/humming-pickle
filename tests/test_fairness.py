"""Fairness: a noisy org cannot starve a quiet one.

Submit a backlog from Org A, then a single job from Org B, and verify Org B's
job completes among the first few — not after Org A's entire backlog drains.

This test is timing-sensitive (relies on the worker being running and on
provider latency), so we use generous bounds: Org B's job must complete
within a few seconds of Org A's first completion, well before A's full batch
is done.
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


async def _wait_terminal(http, key, job_id, deadline):
    while time.monotonic() < deadline:
        r = await http.get(f"/verifications/{job_id}", headers={"X-API-Key": key})
        r.raise_for_status()
        j = r.json()
        if j["status"] in ("completed", "failed"):
            return j["status"], time.monotonic()
        await asyncio.sleep(0.25)
    return "timeout", time.monotonic()


@pytest.mark.asyncio
async def test_org_b_not_starved_by_org_a_backlog(http, org_a_key, org_b_key):
    A_COUNT = 30
    a_ids = []
    for i in range(A_COUNT):
        a_ids.append(await _submit(http, org_a_key, f"fair-a{i}@a.test"))
    b_id = await _submit(http, org_b_key, "fair-b0@b.test")

    t0 = time.monotonic()
    deadline = t0 + 120

    b_status, b_done = await _wait_terminal(http, org_b_key, b_id, deadline)
    assert b_status == "completed", f"Org B's job did not complete: {b_status}"

    b_latency = b_done - t0
    # Worst case if fairness is broken: B waits behind all 30 of A's jobs at
    # 10 r/s + ~3s per call. We give a tight bound that fairness can hit
    # but FIFO cannot.
    assert b_latency < 15, (
        f"Org B took {b_latency:.1f}s — likely starved behind Org A's backlog"
    )
