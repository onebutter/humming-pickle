"""Database access for the API.

Two session factories:

  * `_auth_session()` — short-lived, used only to resolve an API key to an
    org_id. Does NOT set `app.current_org_id`, so any tenant-data query
    issued through it will fail closed (RLS returns zero rows).

  * `get_scoped_session(org_id)` — the only sanctioned way for route
    handlers to read or write tenant data. Opens a transaction, sets the
    `app.current_org_id` GUC for the life of the transaction, and yields
    a session.

The underlying engine is module-private; no raw connection is exposed.
"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_DATABASE_URL = os.environ["DATABASE_URL"]

_engine = create_async_engine(_DATABASE_URL, pool_pre_ping=True, pool_size=10)
_Session = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)


@asynccontextmanager
async def _auth_session() -> AsyncIterator[AsyncSession]:
    """Unscoped session for the API-key → org lookup only."""
    async with _Session() as session:
        yield session


@asynccontextmanager
async def get_scoped_session(org_id: UUID) -> AsyncIterator[AsyncSession]:
    """RLS-scoped session. All tenant-data queries must go through this."""
    async with _Session() as session:
        async with session.begin():
            # set_config(name, value, is_local=true) is the parameterized
            # equivalent of `SET LOCAL`; safer than f-string interpolation.
            await session.execute(
                text("SELECT set_config('app.current_org_id', :v, true)"),
                {"v": str(org_id)},
            )
            yield session


async def resolve_api_key(key_hash: str) -> UUID | None:
    async with _auth_session() as session:
        row = (
            await session.execute(
                text("SELECT org_id FROM api_keys WHERE key_hash = :h"),
                {"h": key_hash},
            )
        ).first()
        return row[0] if row else None
