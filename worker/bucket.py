"""Token bucket.

Capacity tokens accumulate at `rate` per second, up to `capacity`. `acquire()`
takes one token, sleeping if the bucket is empty. Single-process, asyncio-safe.

For multi-worker, this moves to Redis with an atomic refill+decrement Lua
script — same algorithm, same knobs.
"""

import asyncio
import time


class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(
                    self.capacity, self._tokens + elapsed * self.rate
                )
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # Compute how long until the next token; release the lock
                # while we wait so other coroutines can attempt acquire.
                wait_s = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(wait_s)
