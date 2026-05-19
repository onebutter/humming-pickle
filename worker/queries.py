"""SQL for the dispatcher and the reclaimer.

Kept as plain SQL strings so the fairness query is reviewable by eye — it's
the most important piece of logic in the worker.
"""

# Fairness query: pick the next ready job from the org whose `last_served_at`
# is oldest (NULLs first). `FOR UPDATE SKIP LOCKED` claims the row atomically;
# even with multiple workers, no two will claim the same job.
#
# The CTE has two stages: (1) find the longest-waited org with pending work,
# (2) claim its oldest ready job. The outer UPDATE flips it to in_progress,
# bumps `attempts`, and stamps `claimed_at`. RETURNING gives us the row.
CLAIM_NEXT_JOB = """
WITH next_org AS (
  SELECT o.id
  FROM organizations o
  WHERE EXISTS (
    SELECT 1
    FROM verification_jobs j
    WHERE j.org_id = o.id
      AND j.status = 'pending'
      AND j.next_attempt_at <= now()
  )
  ORDER BY o.last_served_at NULLS FIRST
  LIMIT 1
),
claimed AS (
  SELECT j.id
  FROM verification_jobs j, next_org
  WHERE j.org_id = next_org.id
    AND j.status = 'pending'
    AND j.next_attempt_at <= now()
  ORDER BY j.created_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE verification_jobs j
SET status      = 'in_progress',
    claimed_at  = now(),
    attempts    = j.attempts + 1,
    updated_at  = now()
FROM claimed
WHERE j.id = claimed.id
RETURNING j.id, j.org_id, j.subject_email, j.metadata, j.attempts;
"""

# After a successful claim, rotate the org to the back of the fairness line.
BUMP_ORG_SERVED_AT = """
UPDATE organizations SET last_served_at = now() WHERE id = :org_id;
"""

# Terminal success.
COMPLETE_JOB = """
UPDATE verification_jobs
SET status = 'completed',
    result = CAST(:result AS jsonb),
    updated_at = now()
WHERE id = :id;
"""

# Schedule a retry: push back to pending with a future `next_attempt_at`.
RETRY_JOB = """
UPDATE verification_jobs
SET status = 'pending',
    next_attempt_at = now() + (:backoff_seconds || ' seconds')::interval,
    claimed_at = NULL,
    result = CAST(:last_error AS jsonb),
    updated_at = now()
WHERE id = :id;
"""

# Terminal failure (attempts exhausted, or non-retryable error).
FAIL_JOB = """
UPDATE verification_jobs
SET status = 'failed',
    result = CAST(:last_error AS jsonb),
    updated_at = now()
WHERE id = :id;
"""

# Crash recovery. Any row stuck in `in_progress` for longer than the visibility
# timeout is presumed orphaned (provider takes <= 5s; threshold is 60s) and
# gets pushed back to pending. `attempts` is not decremented — the original
# claim already consumed one attempt.
RECLAIM_STUCK = """
UPDATE verification_jobs
SET status = 'pending',
    claimed_at = NULL,
    updated_at = now()
WHERE status = 'in_progress'
  AND claimed_at < now() - (:threshold_seconds || ' seconds')::interval
RETURNING id;
"""
