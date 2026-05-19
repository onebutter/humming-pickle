"""Shared test fixtures.

Tests run against the docker-compose stack — `docker compose up -d` must be
running before `pytest`. They hit the live API and use a direct DB connection
(as `app_api`) to verify RLS behavior end-to-end.
"""

import os

import httpx
import psycopg
import pytest
import pytest_asyncio

API_URL = os.environ.get("API_URL", "http://localhost:8080")
PG_DSN = os.environ.get(
    "PG_DSN", "postgresql://app_api:app_api_pw@localhost:5433/verifications"
)

ORG_A_KEY = "org_a_key"
ORG_B_KEY = "org_b_key"


@pytest.fixture
def org_a_key() -> str:
    return ORG_A_KEY


@pytest.fixture
def org_b_key() -> str:
    return ORG_B_KEY


@pytest_asyncio.fixture
async def http() -> httpx.AsyncClient:
    async with httpx.AsyncClient(base_url=API_URL, timeout=10.0) as c:
        yield c


@pytest.fixture
def pg_app_api() -> psycopg.Connection:
    """Direct connection as the app_api role — used to verify RLS fail-closed
    behavior without going through the API layer."""
    with psycopg.connect(PG_DSN) as conn:
        yield conn
