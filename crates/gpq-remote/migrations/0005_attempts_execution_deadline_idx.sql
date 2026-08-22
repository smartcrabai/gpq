-- Execution-deadline sweep index (ADR 0003, ADR 0013).
--
-- `overdue_executions` (crates/gpq-remote/src/db/attempts.rs) filters
-- `tenant_id = $1 AND state IN ('leased', 'running') AND execution_deadline
-- IS NOT NULL AND execution_deadline <= $2 ORDER BY execution_deadline`, run
-- once per Tenant on every expiry tick (the sweep loop
-- `0003_tenant_enumeration` enumerates Tenants for). None of
-- `attempts_live_lease_idx`, `attempts_worker_idx`, or
-- `attempts_generation_idx` from `0001_initial` cover `execution_deadline`,
-- so this sweep degenerated into a full scan of a table that retains every
-- Attempt ever run.

CREATE INDEX attempts_execution_deadline_idx
    ON attempts (tenant_id, execution_deadline)
    WHERE state IN ('leased', 'running') AND execution_deadline IS NOT NULL;
