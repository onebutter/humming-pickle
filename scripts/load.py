"""Demo load harness.

Submits an asymmetric burst to make fairness visible: Org A gets a big batch,
Org B gets a single job somewhere in the middle. If fairness works, Org B's
job finishes among the first few results — not buried at the back of the
queue.

Usage:
    python scripts/load.py                  # defaults: 100 A, 1 B
    python scripts/load.py --a 200 --b 5    # custom mix
"""

import argparse
import asyncio
import time
from collections import defaultdict
from datetime import datetime, timezone
from uuid import UUID

import httpx

API_URL = "http://localhost:8080"
ORG_KEYS = {"A": "org_a_key", "B": "org_b_key"}


async def submit_one(client: httpx.AsyncClient, org: str, n: int) -> tuple[UUID, datetime]:
    resp = await client.post(
        f"{API_URL}/verifications",
        headers={"X-API-Key": ORG_KEYS[org]},
        json={
            "subject_email": f"user{n}@org-{org.lower()}.test",
            "metadata": {"batch_index": n},
        },
    )
    resp.raise_for_status()
    body = resp.json()
    return UUID(body["id"]), datetime.fromisoformat(body["created_at"])


async def poll_until_terminal(
    client: httpx.AsyncClient, org: str, job_id: UUID, deadline: float
) -> dict:
    while time.monotonic() < deadline:
        resp = await client.get(
            f"{API_URL}/verifications/{job_id}",
            headers={"X-API-Key": ORG_KEYS[org]},
        )
        resp.raise_for_status()
        job = resp.json()
        if job["status"] in ("completed", "failed"):
            return job
        await asyncio.sleep(0.25)
    return {"status": "timeout", "id": str(job_id)}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--a", type=int, default=100, help="jobs for Org A")
    parser.add_argument("--b", type=int, default=1, help="jobs for Org B")
    parser.add_argument("--timeout", type=int, default=180, help="seconds")
    args = parser.parse_args()

    print(f"=== Submitting {args.a} jobs for Org A, then {args.b} for Org B ===")
    submitted: list[tuple[str, UUID, datetime]] = []  # (org, id, created_at)

    async with httpx.AsyncClient(timeout=10.0) as client:
        t0 = time.monotonic()
        for n in range(args.a):
            jid, created = await submit_one(client, "A", n)
            submitted.append(("A", jid, created))
        for n in range(args.b):
            jid, created = await submit_one(client, "B", n)
            submitted.append(("B", jid, created))
        print(f"submitted in {time.monotonic() - t0:.2f}s; polling for results...")

        deadline = time.monotonic() + args.timeout

        async def _poll(org: str, jid: UUID, created_at: datetime) -> dict:
            r = await poll_until_terminal(client, org, jid, deadline)
            r["_org"] = org
            r["_created_at"] = created_at
            return r

        # Poll in parallel — sequential polling would falsely attribute Org B's
        # latency to "after all of A finished," because the loop wouldn't ask
        # about B until it had finished waiting on every A job. The actual
        # truth lives in worker logs; this just makes the summary honest.
        results = await asyncio.gather(
            *[_poll(org, jid, c) for org, jid, c in submitted]
        )

    print("\n=== Summary (latency = job.updated_at - job.created_at) ===")
    by_org: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        by_org[r["_org"]].append(r)
    for org, rs in sorted(by_org.items()):
        completed = [r for r in rs if r["status"] == "completed"]
        failed = [r for r in rs if r["status"] == "failed"]
        timed_out = [r for r in rs if r["status"] == "timeout"]
        if completed:
            def _lat(r: dict) -> float:
                done = datetime.fromisoformat(r["updated_at"])
                return (done - r["_created_at"]).total_seconds()
            latencies = sorted(_lat(r) for r in completed)
            p50 = latencies[len(latencies) // 2]
            p95 = latencies[min(int(len(latencies) * 0.95), len(latencies) - 1)]
            first_done = min(_lat(r) for r in completed)
        else:
            p50 = p95 = first_done = float("nan")
        print(
            f"  Org {org}: submitted={len(rs)}  completed={len(completed)}  "
            f"failed={len(failed)}  timeout={len(timed_out)}  "
            f"first_done={first_done:.2f}s  p50={p50:.2f}s  p95={p95:.2f}s"
        )
    print("\nIf fairness is working, Org B's first_done should be within a")
    print("few seconds of Org A's first_done — not blocked behind A's batch.")
    print("For the dispatch order, see: docker compose logs worker | grep dispatched")


if __name__ == "__main__":
    asyncio.run(main())
