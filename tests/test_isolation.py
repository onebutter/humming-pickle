"""Tenant isolation: Org A's API key cannot read Org B's job by id.

The contract is 404, not 403 — we never confirm existence across tenants.
RLS makes this fall out naturally because the row literally isn't visible
to a query scoped to the wrong org.
"""

import pytest


@pytest.mark.asyncio
async def test_cross_org_get_returns_404(http, org_a_key, org_b_key):
    submit = await http.post(
        "/verifications",
        headers={"X-API-Key": org_a_key},
        json={"subject_email": "alpha@a.test", "metadata": {}},
    )
    assert submit.status_code == 202
    job_id = submit.json()["id"]

    own = await http.get(
        f"/verifications/{job_id}", headers={"X-API-Key": org_a_key}
    )
    assert own.status_code == 200

    cross = await http.get(
        f"/verifications/{job_id}", headers={"X-API-Key": org_b_key}
    )
    assert cross.status_code == 404, "RLS leak — org B could read org A's job"


@pytest.mark.asyncio
async def test_missing_key_is_401(http):
    r = await http.get("/verifications/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_invalid_key_is_401(http):
    r = await http.get(
        "/verifications/00000000-0000-0000-0000-000000000000",
        headers={"X-API-Key": "not-a-real-key"},
    )
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_list_only_returns_own_org(http, org_a_key, org_b_key):
    a_ids = set()
    for i in range(3):
        r = await http.post(
            "/verifications",
            headers={"X-API-Key": org_a_key},
            json={"subject_email": f"a{i}@a.test", "metadata": {}},
        )
        a_ids.add(r.json()["id"])

    b_ids = set()
    for i in range(2):
        r = await http.post(
            "/verifications",
            headers={"X-API-Key": org_b_key},
            json={"subject_email": f"b{i}@b.test", "metadata": {}},
        )
        b_ids.add(r.json()["id"])

    a_list = (
        await http.get("/verifications", headers={"X-API-Key": org_a_key})
    ).json()
    a_visible_ids = {item["id"] for item in a_list["items"]}
    assert b_ids.isdisjoint(a_visible_ids), "RLS leak in list endpoint"
