-- Initial schema.
--
-- ADR 0011: tenant-owned rows share one schema with a mandatory `tenant_id`,
-- composite tenant-safe references, and forced row-level security. Serving runs
-- as the non-owner `gpq_app` role without BYPASSRLS; migration and local
-- administration use `gpq_admin`.
-- ADR 0014: PostgreSQL-specific features are used directly.
-- ADR 0016: only `gpq-remote migrate` holds the schema-owner credential.
-- ADR 0017: no Task entity. UUIDv7 identifiers, TIMESTAMPTZ, text state columns
-- with check constraints, and JSONB only for backend-shaped data.

-- Roles ---------------------------------------------------------------------
-- Created NOLOGIN so operators attach their own login users with GRANT.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gpq_admin') THEN
        CREATE ROLE gpq_admin NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gpq_app') THEN
        CREATE ROLE gpq_app NOLOGIN NOBYPASSRLS;
    END IF;
END
$$;

-- The tenant of the current transaction, set with
-- `select set_config('gpq.tenant_id', $1, true)` inside each tenant-scoped
-- transaction. NULL means "no tenant", which every policy below rejects.
CREATE OR REPLACE FUNCTION gpq_current_tenant() RETURNS uuid
    LANGUAGE sql
    STABLE
    PARALLEL SAFE
AS $$
    SELECT nullif(current_setting('gpq.tenant_id', true), '')::uuid
$$;

-- Tenants -------------------------------------------------------------------
CREATE TABLE tenants (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE,
    maximum_queue_age interval NOT NULL DEFAULT interval '30 minutes'
        CHECK (maximum_queue_age > interval '0'),
    max_queued_generations integer NOT NULL DEFAULT 1000
        CHECK (max_queued_generations > 0),
    max_input_artifact_bytes bigint NOT NULL DEFAULT 268435456
        CHECK (max_input_artifact_bytes > 0),
    max_output_artifact_bytes bigint NOT NULL DEFAULT 2147483648
        CHECK (max_output_artifact_bytes > 0),
    execution_timeout_ceiling interval NOT NULL DEFAULT interval '24 hours'
        CHECK (execution_timeout_ceiling > interval '0'),
    default_priority smallint NOT NULL DEFAULT 5
        CHECK (default_priority BETWEEN 0 AND 9),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    deleted_at timestamptz
);

-- Tenant Master Keys are stored only as keyed hashes and may overlap during
-- rotation (ADR 0009).
CREATE TABLE tenant_master_keys (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    key_hash bytea NOT NULL,
    label text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    revoked_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (key_hash)
);

CREATE INDEX tenant_master_keys_live_idx
    ON tenant_master_keys (key_hash)
    WHERE revoked_at IS NULL;

-- Workers -------------------------------------------------------------------
CREATE TABLE workers (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    name text NOT NULL,
    host_descriptor text NOT NULL DEFAULT '',
    worker_version text NOT NULL DEFAULT '',
    protocol_major integer NOT NULL DEFAULT 0,
    protocol_minor integer NOT NULL DEFAULT 0,
    -- Worker Credential, stored as a keyed hash like Master Keys (ADR 0009).
    credential_hash bytea NOT NULL,
    session_id text,
    enrolled_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz,
    revoked_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, name),
    UNIQUE (credential_hash)
);

CREATE INDEX workers_live_credential_idx
    ON workers (credential_hash)
    WHERE revoked_at IS NULL;

-- A non-overlapping set of GPUs with at most one Active Runtime (ADR 0005).
CREATE TABLE device_pools (
    tenant_id uuid NOT NULL,
    id uuid NOT NULL,
    worker_id uuid NOT NULL,
    -- Pool identity as configured on the Worker host.
    pool_key text NOT NULL,
    backend_kind text NOT NULL CHECK (backend_kind IN ('llama_cpp', 'comfyui')),
    backend_version text NOT NULL DEFAULT '',
    ready boolean NOT NULL DEFAULT false,
    unready_reason text NOT NULL DEFAULT '',
    total_slots integer NOT NULL DEFAULT 0 CHECK (total_slots >= 0),
    free_slots integer NOT NULL DEFAULT 0 CHECK (free_slots >= 0),
    resident_model_sha256 text CHECK (resident_model_sha256 ~ '^[0-9a-f]{64}$'),
    accelerator_memory_bytes bigint CHECK (accelerator_memory_bytes > 0),
    custom_nodes jsonb NOT NULL DEFAULT '{}'::jsonb,
    probes jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, worker_id, pool_key),
    FOREIGN KEY (tenant_id, worker_id) REFERENCES workers (tenant_id, id) ON DELETE CASCADE,
    CHECK (free_slots <= total_slots)
);

-- Model material a Pool advertises, by content hash (ADR 0012).
CREATE TABLE pool_models (
    tenant_id uuid NOT NULL,
    pool_id uuid NOT NULL,
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    -- Set when this Pool proved incapable, e.g. by running out of memory
    -- during an Attempt (ADR 0003).
    incapable_since timestamptz,
    PRIMARY KEY (tenant_id, pool_id, content_sha256),
    FOREIGN KEY (tenant_id, pool_id) REFERENCES device_pools (tenant_id, id) ON DELETE CASCADE
);

-- Catalog -------------------------------------------------------------------
CREATE TABLE model_versions (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    modality text NOT NULL CHECK (modality IN ('llm', 'image', 'video', 'music')),
    execution_timeout interval CHECK (execution_timeout > interval '0'),
    estimated_vram_bytes bigint CHECK (estimated_vram_bytes > 0),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, content_sha256)
);

CREATE TABLE model_aliases (
    tenant_id uuid NOT NULL,
    alias text NOT NULL,
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, alias),
    FOREIGN KEY (tenant_id, content_sha256)
        REFERENCES model_versions (tenant_id, content_sha256) ON DELETE RESTRICT
);

CREATE TABLE workflow_versions (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    modality text NOT NULL CHECK (modality IN ('llm', 'image', 'video', 'music')),
    -- Opaque ComfyUI API-format graph (ADR 0007).
    graph jsonb NOT NULL,
    -- Output and execution manifest (ADR 0007).
    manifest jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, content_sha256)
);

CREATE TABLE workflow_aliases (
    tenant_id uuid NOT NULL,
    alias text NOT NULL,
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, alias),
    FOREIGN KEY (tenant_id, content_sha256)
        REFERENCES workflow_versions (tenant_id, content_sha256) ON DELETE RESTRICT
);

-- Generations ---------------------------------------------------------------
CREATE TABLE generations (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    state text NOT NULL CHECK (state IN (
        'queued', 'running', 'cancelling', 'succeeded', 'failed', 'cancelled', 'expired'
    )),
    modality text NOT NULL CHECK (modality IN ('llm', 'image', 'video', 'music')),
    -- Whether the caller holds a connection open (ADR 0003).
    caller_kind text NOT NULL CHECK (caller_kind IN ('synchronous', 'durable')),
    target_kind text NOT NULL CHECK (target_kind IN ('model', 'workflow')),
    alias text NOT NULL,
    -- Version pinned at admission; every Attempt reuses it (ADR 0012).
    version_sha256 text NOT NULL CHECK (version_sha256 ~ '^[0-9a-f]{64}$'),
    -- Opaque backend-shaped payload (ADR 0007).
    parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
    priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 9),
    seed bigint,
    execution_timeout interval NOT NULL CHECK (execution_timeout > interval '0'),
    output_placement text NOT NULL
        CHECK (output_placement IN ('object_store', 'worker_local', 'inline_relay')),
    stream_tokens boolean NOT NULL DEFAULT false,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    -- The first successful Attempt; later duplicates never replace it (ADR 0003).
    accepted_attempt_id uuid,
    output_text text NOT NULL DEFAULT '',
    usage jsonb,
    latest_progress jsonb,
    failure_kind text CHECK (failure_kind IN (
        'invalid_input', 'unsupported_capability', 'model_unavailable', 'out_of_memory',
        'backend_crashed', 'execution_timed_out', 'cancelled', 'transfer_failed',
        'internal', 'worker_lost', 'lease_expired'
    )),
    failure_message text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    terminated_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    CHECK (state <> 'succeeded' OR accepted_attempt_id IS NOT NULL),
    CHECK (state <> 'failed' OR failure_kind IS NOT NULL)
);

-- The scheduler's hot path: queued work of one Tenant, oldest first
-- (ADR 0002, ADR 0013).
CREATE INDEX generations_queue_idx
    ON generations (tenant_id, created_at, priority DESC)
    WHERE state = 'queued';

CREATE INDEX generations_queue_version_idx
    ON generations (tenant_id, version_sha256, created_at)
    WHERE state = 'queued';

CREATE INDEX generations_active_idx
    ON generations (tenant_id, state, updated_at)
    WHERE state IN ('running', 'cancelling');

CREATE INDEX generations_listing_idx
    ON generations (tenant_id, created_at DESC);

CREATE TABLE attempts (
    tenant_id uuid NOT NULL,
    id uuid NOT NULL,
    generation_id uuid NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number BETWEEN 1 AND 3),
    state text NOT NULL CHECK (state IN (
        'leased', 'running', 'succeeded', 'failed', 'cancelled', 'lease_expired'
    )),
    worker_id uuid NOT NULL,
    pool_id uuid NOT NULL,
    slot_key text NOT NULL,
    -- Lease expiry in database time; heartbeats push it forward (ADR 0003).
    lease_expires_at timestamptz NOT NULL,
    execution_deadline timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    last_heartbeat_at timestamptz,
    failure_kind text CHECK (failure_kind IN (
        'invalid_input', 'unsupported_capability', 'model_unavailable', 'out_of_memory',
        'backend_crashed', 'execution_timed_out', 'cancelled', 'transfer_failed',
        'internal', 'worker_lost', 'lease_expired'
    )),
    failure_message text NOT NULL DEFAULT '',
    worker_retry_hint boolean NOT NULL DEFAULT false,
    cancel_requested_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, generation_id, attempt_number),
    FOREIGN KEY (tenant_id, generation_id) REFERENCES generations (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, worker_id) REFERENCES workers (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, pool_id) REFERENCES device_pools (tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE generations
    ADD CONSTRAINT generations_accepted_attempt_fkey
    FOREIGN KEY (tenant_id, accepted_attempt_id) REFERENCES attempts (tenant_id, id)
    ON DELETE SET NULL;

-- Lease expiry sweep and heartbeat renewal (ADR 0003).
CREATE INDEX attempts_live_lease_idx
    ON attempts (tenant_id, lease_expires_at)
    WHERE state IN ('leased', 'running');

CREATE INDEX attempts_worker_idx
    ON attempts (tenant_id, worker_id, state);

CREATE INDEX attempts_generation_idx
    ON attempts (tenant_id, generation_id, attempt_number);

-- Artifacts -----------------------------------------------------------------
CREATE TABLE artifacts (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    id uuid NOT NULL,
    generation_id uuid,
    attempt_id uuid,
    direction text NOT NULL CHECK (direction IN ('input', 'output')),
    state text NOT NULL CHECK (state IN (
        'pending', 'available', 'delivering', 'consumed', 'expired', 'lost'
    )),
    placement text NOT NULL
        CHECK (placement IN ('object_store', 'worker_local', 'inline_relay')),
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    digest_sha256 text NOT NULL CHECK (digest_sha256 ~ '^[0-9a-f]{64}$'),
    kind text NOT NULL CHECK (kind IN ('image', 'video', 'audio', 'text', 'binary')),
    mime_type text NOT NULL,
    -- Object storage key when placement is object_store; Remote alone holds the
    -- credentials (ADR 0008).
    object_key text,
    -- Producing Worker when placement is worker_local.
    worker_id uuid,
    delivery_token text,
    -- Bytes already accepted, so an interrupted delivery can resume (ADR 0008).
    committed_offset bigint NOT NULL DEFAULT 0 CHECK (committed_offset >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz,
    -- Unclaimed outputs expire one hour after completion (ADR 0008).
    expires_at timestamptz,
    terminated_at timestamptz,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, generation_id) REFERENCES generations (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, attempt_id) REFERENCES attempts (tenant_id, id) ON DELETE SET NULL,
    FOREIGN KEY (tenant_id, worker_id) REFERENCES workers (tenant_id, id) ON DELETE SET NULL,
    CHECK (placement <> 'object_store' OR object_key IS NOT NULL),
    CHECK (placement <> 'worker_local' OR worker_id IS NOT NULL)
);

CREATE INDEX artifacts_generation_idx
    ON artifacts (tenant_id, generation_id, direction);

CREATE INDEX artifacts_expiry_idx
    ON artifacts (tenant_id, expires_at)
    WHERE state IN ('pending', 'available', 'delivering');

-- Idempotency ---------------------------------------------------------------
-- Native creation carries idempotency in request metadata (ADR 0006).
CREATE TABLE idempotency_keys (
    tenant_id uuid NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    key text NOT NULL,
    request_digest bytea NOT NULL,
    generation_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key),
    FOREIGN KEY (tenant_id, generation_id) REFERENCES generations (tenant_id, id) ON DELETE CASCADE
);

-- Generation events ---------------------------------------------------------
-- State transitions and progress snapshots are retained; token deltas and
-- transport frames are not (ADR 0008).
CREATE TABLE generation_events (
    tenant_id uuid NOT NULL,
    id uuid NOT NULL,
    generation_id uuid NOT NULL,
    sequence bigint NOT NULL,
    kind text NOT NULL CHECK (kind IN ('state_changed', 'progress', 'attempt_created')),
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id),
    UNIQUE (tenant_id, generation_id, sequence),
    FOREIGN KEY (tenant_id, generation_id) REFERENCES generations (tenant_id, id) ON DELETE CASCADE
);

CREATE INDEX generation_events_stream_idx
    ON generation_events (tenant_id, generation_id, sequence);

-- Queue wakeups -------------------------------------------------------------
-- LISTEN/NOTIFY only wakes waiting sessions; a periodic fallback query covers
-- lost notifications because they are not durable (ADR 0013).
CREATE FUNCTION gpq_notify_queued() RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    IF new.state = 'queued' THEN
        PERFORM pg_notify('gpq_queue', new.tenant_id::text);
    END IF;
    RETURN NULL;
END
$$;

CREATE TRIGGER generations_notify_queued
    AFTER INSERT OR UPDATE OF state ON generations
    FOR EACH ROW
    EXECUTE FUNCTION gpq_notify_queued();

-- Row-level security --------------------------------------------------------
-- Forced so that even the table owner is filtered; the serving role has no
-- BYPASSRLS (ADR 0011).
DO $$
DECLARE
    tenant_table text;
BEGIN
    FOREACH tenant_table IN ARRAY ARRAY[
        'tenant_master_keys', 'workers', 'device_pools', 'pool_models',
        'model_versions', 'model_aliases', 'workflow_versions', 'workflow_aliases',
        'generations', 'attempts', 'artifacts', 'idempotency_keys', 'generation_events'
    ]
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tenant_table);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tenant_table);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I FOR ALL TO gpq_app '
            'USING (tenant_id = gpq_current_tenant()) '
            'WITH CHECK (tenant_id = gpq_current_tenant())',
            tenant_table
        );
        EXECUTE format(
            'CREATE POLICY administration ON %I FOR ALL TO gpq_admin USING (true) WITH CHECK (true)',
            tenant_table
        );
    END LOOP;
END
$$;

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenants
    FOR ALL TO gpq_app
    USING (id = gpq_current_tenant())
    WITH CHECK (id = gpq_current_tenant());

CREATE POLICY administration ON tenants
    FOR ALL TO gpq_admin
    USING (true)
    WITH CHECK (true);

-- Privileges ----------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO gpq_app, gpq_admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO gpq_app;
GRANT ALL ON ALL TABLES IN SCHEMA public TO gpq_admin;
GRANT EXECUTE ON FUNCTION gpq_current_tenant() TO gpq_app, gpq_admin;
