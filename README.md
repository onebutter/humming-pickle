# Multi-Tenant Verification API

A small async job-processing service: clients from multiple organizations
submit verification requests, and a worker drains them against a flaky
third-party provider at a strict 10 req/s — fairly across orgs, with
crash-safe state in Postgres and tenant isolation enforced at the row level.

See [`DESIGN.md`](./DESIGN.md) (renders on GitHub) or
[`DESIGN.html`](./DESIGN.html) (open in a browser) for the architecture
and the explicit list of what was implemented vs. cut.

## Quickstart

```bash
docker compose up --build
```

Then **open <http://localhost:8080/> in a browser.** That's the fastest way
to see the system working end-to-end — it's a live dashboard with submit
controls for both orgs, per-org status panels, and an interleaved activity
feed.

### What the dashboard shows

- **Two side-by-side panels** — one per tenant org. Each panel fetches with
  its own API key, so the UI is RLS-scoped on the backend and incidentally
  proves tenant isolation (Org A's panel literally cannot see Org B's jobs).
- **A compact dot grid per panel** — one circle per job, oldest on the left,
  newest on the right. Color = status: gray pending, yellow in-progress,
  orange retrying (between attempts), green completed, red failed.
- **Activity feed** — every submitted / dispatched / completed / failed /
  retry event from both orgs, interleaved by time. Fairness is visible as
  alternating org tags.
- **Presets** — one-click load scenarios (Fairness 30A+1B, Balanced 5A+5B,
  Storm 100A+5B). The Reset button in the header wipes both orgs.

Polls the API every 750ms. The dashboard is served same-origin from the API
service, so no CORS setup is required.

### What's running

| Service       | Port | What it is                                                  |
| ------------- | ---- | ----------------------------------------------------------- |
| `postgres`    | 5433 | Schema, RLS policies, seed orgs/keys loaded from `db/init.sql` on first boot (host port 5433 → container 5432 to avoid clashing with a local Postgres) |
| `enrichment`  | 8081 | Fake third-party provider — 1–5s latency, 20% 5xx, enforces 10 r/s globally |
| `api`         | 8080 | FastAPI service: three verification endpoints + the dashboard at `/` |
| `worker`      | —    | Dispatcher + reclaimer; logs to stdout                      |

The API is ready when `curl http://localhost:8080/health` returns `{"ok":true}`.

### Seeded API keys

Two orgs are seeded on first boot. Their plaintext keys are committed
intentionally (they're for the demo only):

- Org A: `X-API-Key: org_a_key`
- Org B: `X-API-Key: org_b_key`

### Try a request from the command line

```bash
curl -X POST http://localhost:8080/verifications \
  -H "X-API-Key: org_a_key" \
  -H "Content-Type: application/json" \
  -d '{"subject_email":"alice@example.com","metadata":{"source":"signup"}}'
```

Returns a job id immediately. Fetch its status:

```bash
curl http://localhost:8080/verifications/<id> -H "X-API-Key: org_a_key"
```

### Tail raw worker logs

```bash
docker compose logs -f worker
```

Every claim, dispatch, retry, completion, and reclaim is logged as one
structured JSON line per event with `job_id`, `org_id`, and `attempts`.
Useful when the dashboard isn't granular enough — e.g. to see the exact
millisecond ordering of dispatches across orgs.

## Load harness (CLI alternative to the dashboard)

For a non-interactive demo, the load harness submits a deliberately skewed
mix (default: 100 jobs for Org A, then 1 for Org B) and prints per-org
throughput and first-completion timestamps. If fairness is working, Org B's
job completes among the first few, not after Org A's batch finishes.

```bash
pip install -r scripts/requirements.txt
python scripts/load.py
# or: python scripts/load.py --a 200 --b 5
```

You'll see something like:

```
=== Summary ===
  Org A: submitted=100  completed=100  failed=0  first_done_at=3.12s   p50=5.87s   p95=11.23s
  Org B: submitted=1    completed=1    failed=0  first_done_at=4.04s   p50=4.04s   p95=4.04s
```

Org B's `first_done_at` is within a few seconds of Org A's first completion —
proof that A's backlog doesn't starve B.

## Run the tests

```bash
docker compose up -d
pip install -r tests/requirements.txt
pytest tests/
```

Four test files:

| File                          | What it pins down                                                 |
| ----------------------------- | ----------------------------------------------------------------- |
| `test_isolation.py`           | Cross-org GET returns 404 (not 403, not 200). List is org-scoped. |
| `test_rls_fails_closed.py`    | A query with no `app.current_org_id` GUC returns zero rows.       |
| `test_fairness.py`            | Org B's lone job isn't blocked behind Org A's 30-job backlog.     |
| `test_retry.py`               | At least one job retries under the 20% flaky provider.            |

`test_fairness.py` and `test_retry.py` are timing-sensitive (they exercise the
running worker). Bounds are loose; if a test machine is unusually slow they
may need adjustment.

## Environment

All config is via `docker-compose.yml`. Key knobs:

| Env var                  | Default | Where        | Meaning                                   |
| ------------------------ | ------- | ------------ | ----------------------------------------- |
| `RATE_LIMIT_PER_SEC`     | `10`    | enrichment, worker | Global ceiling, in jobs/sec         |
| `RATE_LIMIT_BURST`       | `10`    | worker       | Token bucket capacity                     |
| `FAILURE_RATE`           | `0.20`  | enrichment   | Fraction of requests returning 502        |
| `MIN_LATENCY_MS`         | `1000`  | enrichment   | Provider response latency floor           |
| `MAX_LATENCY_MS`         | `5000`  | enrichment   | Provider response latency ceiling         |
| `MAX_ATTEMPTS`           | `5`     | worker       | Retries before a job goes `failed`        |
| `RECLAIM_AFTER_SECONDS`  | `60`    | worker       | Visibility timeout for `in_progress` rows |

Postgres data lives only inside the container — `docker compose down -v`
gives you a clean slate.

## Reset

```bash
docker compose down -v && docker compose up --build
```

---

_Built as a 3–4 hour take-home exercise; the scope cuts are listed in
[`DESIGN.md`](./DESIGN.md) §11._
