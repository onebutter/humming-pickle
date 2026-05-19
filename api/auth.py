"""API-key auth. Resolves `X-API-Key` to an `org_id` (UUID) or 401s.

The dependency itself returns just the org_id; route handlers ask for a
scoped session separately via `Depends(scoped_session)`. Keeping these
two responsibilities separate makes it impossible to issue a query before
authentication has completed.
"""

import hashlib
from collections.abc import AsyncIterator
from uuid import UUID

from fastapi import Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_scoped_session, resolve_api_key


def _hash_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode()).hexdigest()


async def current_org(x_api_key: str | None = Header(default=None)) -> UUID:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="missing X-API-Key header")
    org_id = await resolve_api_key(_hash_key(x_api_key))
    if org_id is None:
        raise HTTPException(status_code=401, detail="invalid API key")
    return org_id


async def scoped_session(
    org_id: UUID = Depends(current_org),
) -> AsyncIterator[AsyncSession]:
    async with get_scoped_session(org_id) as session:
        yield session
