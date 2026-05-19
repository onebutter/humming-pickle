"""Verification API.

Three endpoints, all RLS-scoped via the `scoped_session` dependency. Cross-org
access returns 404 (not 403) so we don't leak which job ids exist in other
tenants — RLS makes this fall out for free because the row simply isn't
visible to a query scoped to the wrong org.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from auth import current_org, scoped_session

app = FastAPI(title="verification-api")


@app.get("/", include_in_schema=False)
async def ui():
    """Demo dashboard. Served same-origin so the page can call the API
    endpoints without CORS gymnastics."""
    return FileResponse("ui.html")


class SubmitBody(BaseModel):
    subject_email: str
    metadata: dict[str, Any] = {}


class JobOut(BaseModel):
    id: UUID
    status: str
    subject_email: str
    metadata: dict[str, Any]
    attempts: int
    result: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime


class JobListOut(BaseModel):
    items: list[JobOut]
    page: int
    page_size: int
    total: int


def _row_to_job(row: Any) -> JobOut:
    return JobOut(
        id=row.id,
        status=row.status,
        subject_email=row.subject_email,
        metadata=row.metadata,
        attempts=row.attempts,
        result=row.result,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@app.post("/verifications", status_code=202)
async def submit(
    body: SubmitBody,
    org_id: UUID = Depends(current_org),
    session: AsyncSession = Depends(scoped_session),
) -> JobOut:
    row = (
        await session.execute(
            text(
                """
                INSERT INTO verification_jobs (org_id, subject_email, metadata)
                VALUES (:org_id, :subject_email, CAST(:metadata AS jsonb))
                RETURNING id, status, subject_email, metadata, attempts,
                          result, created_at, updated_at
                """
            ),
            {
                "org_id": str(org_id),
                "subject_email": body.subject_email,
                "metadata": __import__("json").dumps(body.metadata),
            },
        )
    ).one()
    return _row_to_job(row)


@app.get("/verifications/{job_id}")
async def get_one(
    job_id: UUID,
    session: AsyncSession = Depends(scoped_session),
) -> JobOut:
    row = (
        await session.execute(
            text(
                """
                SELECT id, status, subject_email, metadata, attempts,
                       result, created_at, updated_at
                FROM verification_jobs
                WHERE id = :id
                """
            ),
            {"id": str(job_id)},
        )
    ).first()
    if row is None:
        # RLS already hides cross-org rows; this 404 also covers the
        # genuinely-missing case. We never distinguish the two.
        raise HTTPException(status_code=404, detail="not found")
    return _row_to_job(row)


@app.get("/verifications")
async def list_jobs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(scoped_session),
) -> JobListOut:
    offset = (page - 1) * page_size
    rows = (
        await session.execute(
            text(
                """
                SELECT id, status, subject_email, metadata, attempts,
                       result, created_at, updated_at
                FROM verification_jobs
                ORDER BY created_at DESC, id DESC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"limit": page_size, "offset": offset},
        )
    ).all()
    total = (
        await session.execute(text("SELECT count(*) FROM verification_jobs"))
    ).scalar_one()
    return JobListOut(
        items=[_row_to_job(r) for r in rows],
        page=page,
        page_size=page_size,
        total=int(total),
    )


@app.delete("/verifications", status_code=204)
async def clear_all(session: AsyncSession = Depends(scoped_session)) -> None:
    """Demo-only: wipe this org's jobs.

    RLS keeps the DELETE scoped to the calling org — there is no way for
    Org A to wipe Org B's data via this endpoint. Worker writes to
    in-progress rows that just got deleted simply affect zero rows —
    no crash, just a ghost dispatch in the logs.

    `organizations.last_served_at` is intentionally not reset: fairness is
    "longest-waited org wins" relative to other orgs, not absolute, so
    stale-but-equal timestamps from a previous run still produce correct
    interleaving on the next batch.
    """
    await session.execute(text("DELETE FROM verification_jobs"))


@app.get("/health")
async def health():
    return {"ok": True}
