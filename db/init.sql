-- ===========================================================================
-- Schema, RLS policies, and seed data for the verification service.
-- Runs once on first Postgres container boot via /docker-entrypoint-initdb.d.
-- ===========================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- Roles
--
-- Two least-privilege roles. The API container connects as app_api (no
-- BYPASSRLS); the worker connects as app_worker (BYPASSRLS) because the
-- fairness scheduler needs cross-org visibility. The privilege boundary
-- lives in the database, not the application code.
-- ---------------------------------------------------------------------------

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_api') THEN
    CREATE ROLE app_api LOGIN PASSWORD 'app_api_pw';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_worker') THEN
    CREATE ROLE app_worker LOGIN PASSWORD 'app_worker_pw' BYPASSRLS;
  END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Tables
-- ---------------------------------------------------------------------------

CREATE TABLE organizations (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name            text NOT NULL,
  last_served_at  timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now()
);

-- Auth table. Looked up by key_hash, which is itself the credential —
-- no RLS, separated from tenant data so the auth bootstrap is trivial.
CREATE TABLE api_keys (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id      uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  key_hash    text NOT NULL UNIQUE,
  label       text,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE verification_jobs (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id           uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  subject_email    text NOT NULL,
  metadata         jsonb NOT NULL DEFAULT '{}'::jsonb,
  status           text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','in_progress','completed','failed')),
  attempts         int  NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  claimed_at       timestamptz,
  result           jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX jobs_org_id_desc      ON verification_jobs (org_id, id DESC);
CREATE INDEX jobs_pending_ready    ON verification_jobs (next_attempt_at)
  WHERE status = 'pending';
CREATE INDEX jobs_inflight_claimed ON verification_jobs (claimed_at)
  WHERE status = 'in_progress';

-- ---------------------------------------------------------------------------
-- Row-Level Security on verification_jobs (the only tenant-data table)
--
-- Fail-closed: the policy uses the two-arg current_setting('...', true)
-- which returns NULL when the GUC is unset. NULL = anything → NULL → not
-- true → no rows match. Forgetting to SET LOCAL returns empty, never leaks.
-- ---------------------------------------------------------------------------

ALTER TABLE verification_jobs ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_select ON verification_jobs
  FOR SELECT
  USING (org_id = current_setting('app.current_org_id', true)::uuid);

CREATE POLICY tenant_isolation_insert ON verification_jobs
  FOR INSERT
  WITH CHECK (org_id = current_setting('app.current_org_id', true)::uuid);

CREATE POLICY tenant_isolation_update ON verification_jobs
  FOR UPDATE
  USING (org_id = current_setting('app.current_org_id', true)::uuid)
  WITH CHECK (org_id = current_setting('app.current_org_id', true)::uuid);

CREATE POLICY tenant_isolation_delete ON verification_jobs
  FOR DELETE
  USING (org_id = current_setting('app.current_org_id', true)::uuid);

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE, DELETE ON verification_jobs TO app_api;
GRANT SELECT ON api_keys              TO app_api;
GRANT SELECT ON organizations         TO app_api;

GRANT SELECT, INSERT, UPDATE, DELETE ON verification_jobs TO app_worker;
GRANT SELECT, UPDATE                  ON organizations    TO app_worker;
GRANT SELECT                          ON api_keys         TO app_worker;

-- ---------------------------------------------------------------------------
-- Seed data
--
-- Two orgs with hardcoded keys for the demo. In production these would be
-- created via an admin flow; here they're deterministic so the load script
-- and tests can authenticate without bootstrap dance.
--
-- Key hashes are sha256 of the plaintext key.
--   org_a_key → 6c1f8...  (computed below via digest())
--   org_b_key → ...
-- ---------------------------------------------------------------------------

INSERT INTO organizations (id, name) VALUES
  ('00000000-0000-0000-0000-00000000000a', 'Org A'),
  ('00000000-0000-0000-0000-00000000000b', 'Org B');

INSERT INTO api_keys (org_id, key_hash, label) VALUES
  ('00000000-0000-0000-0000-00000000000a',
   encode(digest('org_a_key', 'sha256'), 'hex'),
   'demo'),
  ('00000000-0000-0000-0000-00000000000b',
   encode(digest('org_b_key', 'sha256'), 'hex'),
   'demo');
