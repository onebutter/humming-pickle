"""Fake third-party enrichment provider.

Simulates the real-world conditions the verification worker has to cope with:
1-5s latency, ~20% transient 5xx failures, and a strict global 10 req/s
ceiling. The ceiling is enforced here (not just stated) so a reviewer can
verify the worker actually respects it rather than taking our word for it.
"""

import asyncio
import os
import random
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

RATE_LIMIT_PER_SEC = int(os.environ.get("RATE_LIMIT_PER_SEC", "10"))
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.20"))
MIN_LATENCY_MS = int(os.environ.get("MIN_LATENCY_MS", "1000"))
MAX_LATENCY_MS = int(os.environ.get("MAX_LATENCY_MS", "5000"))

app = FastAPI(title="enrichment-provider")


class _GlobalRateLimiter:
    """Sliding-window counter over the most recent 1.0s of request starts."""

    def __init__(self, limit: int):
        self.limit = limit
        self.timestamps: list[float] = []
        self.lock = asyncio.Lock()

    async def try_acquire(self) -> bool:
        async with self.lock:
            now = time.monotonic()
            cutoff = now - 1.0
            self.timestamps = [t for t in self.timestamps if t > cutoff]
            if len(self.timestamps) >= self.limit:
                return False
            self.timestamps.append(now)
            return True


limiter = _GlobalRateLimiter(RATE_LIMIT_PER_SEC)


class EnrichRequest(BaseModel):
    subject_email: str
    metadata: dict | None = None


@app.post("/enrich")
async def enrich(req: EnrichRequest):
    if not await limiter.try_acquire():
        raise HTTPException(status_code=429, detail="rate limit exceeded")

    latency_ms = random.randint(MIN_LATENCY_MS, MAX_LATENCY_MS)
    await asyncio.sleep(latency_ms / 1000.0)

    if random.random() < FAILURE_RATE:
        raise HTTPException(status_code=502, detail="transient upstream failure")

    return {
        "subject_email": req.subject_email,
        "verified": True,
        "risk_score": round(random.random(), 3),
        "latency_ms": latency_ms,
    }


@app.get("/health")
async def health():
    return {"ok": True}
