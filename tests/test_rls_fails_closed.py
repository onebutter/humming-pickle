"""RLS fail-closed: without `SET LOCAL app.current_org_id`, queries return zero
rows — they do not leak data.

This is the regression test that catches the day a future refactor forgets to
set the GUC. Without it, RLS failing closed would be silent and the leak
(none, in this case, because closed = empty) would only be noticed when
customers complained that their queries returned nothing. With it, the
guarantee is locked in.
"""

import pytest


@pytest.mark.asyncio
async def test_query_without_guc_returns_zero_rows(pg_app_api, http, org_a_key):
    submit = await http.post(
        "/verifications",
        headers={"X-API-Key": org_a_key},
        json={"subject_email": "guc-test@a.test", "metadata": {}},
    )
    assert submit.status_code == 202

    with pg_app_api.cursor() as cur:
        cur.execute("SELECT id FROM verification_jobs")
        rows = cur.fetchall()
    assert rows == [], (
        f"RLS did not fail closed: app_api saw {len(rows)} rows without GUC"
    )


@pytest.mark.asyncio
async def test_query_with_correct_guc_returns_rows(pg_app_api, http, org_a_key):
    submit = await http.post(
        "/verifications",
        headers={"X-API-Key": org_a_key},
        json={"subject_email": "guc-test-2@a.test", "metadata": {}},
    )
    assert submit.status_code == 202

    org_a_id = "00000000-0000-0000-0000-00000000000a"
    with pg_app_api.transaction():
        with pg_app_api.cursor() as cur:
            cur.execute("SELECT set_config('app.current_org_id', %s, true)", (org_a_id,))
            cur.execute("SELECT id FROM verification_jobs")
            rows = cur.fetchall()
    assert len(rows) > 0, "GUC set but RLS still hid all rows"
