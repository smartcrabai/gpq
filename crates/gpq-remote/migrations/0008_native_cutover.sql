-- Native-only cutover (forward migration; 0001-0007 are already applied).
--
-- The OpenAI-compatible LLM surface, the ComfyUI-compatible prompt surface,
-- Workflow catalog entries, raw ComfyUI prompts, and the llama.cpp /
-- mlx-dspark / ComfyUI runtime kinds are removed. Native generation is image
-- and video only, pinned to an immutable ModelManifest by content hash.
--
-- This migration NEVER deletes user history. Legacy generations, attempts,
-- artifacts, events, idempotency keys, and catalog rows cannot be represented
-- by the native schema, so the migration refuses to run while any of them
-- exist. Archive or export that history, then migrate a fresh (or emptied)
-- database. The transaction rolls back, leaving the old database untouched.
--
-- Tenants, Master Keys, Workers, enrollment, and Device Pools are preserved.
-- Device Pools are re-advertised on the next capability report.

-- Guard ------------------------------------------------------------------------
-- The migration runner must own the schema and altered objects (or be a
-- superuser). For all-tenant history counts it must have BYPASSRLS or effective,
-- inherited gpq_admin privileges. The explicit role guard prevents RLS from
-- silently treating hidden history as empty.
DO $$
DECLARE
    guarded text;
    legacy_rows bigint;
BEGIN
    -- `MEMBER` also accepts NOINHERIT membership, which does not activate the
    -- administration policy for this session.
    IF NOT (
        EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user AND rolbypassrls)
        OR pg_has_role(current_user, 'gpq_admin', 'USAGE')
    ) THEN
        RAISE EXCEPTION 'gpq native cutover requires effective gpq_admin privileges or BYPASSRLS'
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    FOREACH guarded IN ARRAY ARRAY[
        'generations', 'attempts', 'artifacts', 'idempotency_keys',
        'generation_events', 'comfy_prompts', 'workflow_versions',
        'workflow_aliases', 'model_versions', 'model_aliases', 'pool_models'
    ]
    LOOP
        EXECUTE format('SELECT count(*) FROM %I', guarded) INTO legacy_rows;
        IF legacy_rows > 0 THEN
            RAISE EXCEPTION
                'gpq native cutover refused: table % holds % legacy row(s). Archive or export them and migrate a fresh database; this migration deletes nothing.',
                guarded, legacy_rows
                USING ERRCODE = 'feature_not_supported';
        END IF;
    END LOOP;
END
$$;

-- Obsolete tables (verified empty above) ---------------------------------------
DROP TABLE comfy_prompts;
DROP TABLE workflow_aliases;
DROP TABLE workflow_versions;

-- Catalog ---------------------------------------------------------------------
ALTER TABLE model_versions
    DROP COLUMN modality,
    ADD COLUMN family text NOT NULL CHECK (family IN ('qwen_image_2_1', 'minimax_h3')),
    ADD COLUMN modality text NOT NULL CHECK (modality IN ('image', 'video')),
    -- Canonical serialized ModelManifest; its SHA-256 is content_sha256 (ADR 0012).
    ADD COLUMN manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object');

-- The domain limit is the memory a version needs, not a VRAM-specific figure.
ALTER TABLE model_versions RENAME COLUMN estimated_vram_bytes TO estimated_memory_bytes;
ALTER TABLE device_pools
    DROP CONSTRAINT device_pools_accelerator_memory_bytes_check,
    ADD CONSTRAINT device_pools_accelerator_memory_bytes_check
        CHECK (accelerator_memory_bytes >= 0);

-- Device Pools ----------------------------------------------------------------
ALTER TABLE device_pools
    DROP CONSTRAINT device_pools_backend_kind_check,
    DROP COLUMN custom_nodes,
    ADD COLUMN shape_limits jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(shape_limits) = 'object');

ALTER TABLE device_pools RENAME COLUMN backend_kind TO accelerator;
UPDATE device_pools SET accelerator = '';
ALTER TABLE device_pools
    ADD CONSTRAINT device_pools_accelerator_check CHECK (length(accelerator) <= 64),
    ADD COLUMN image_lora_sha256 text[] NOT NULL DEFAULT '{}';

-- Runtime OOM never persists capability: retry exclusion is scoped to the
-- Generation (ADR 0003), so the obsolete per-Pool marker is dropped.
ALTER TABLE pool_models DROP COLUMN incapable_since;

-- Generations -----------------------------------------------------------------
ALTER TABLE generations
    DROP CONSTRAINT generations_modality_check,
    DROP CONSTRAINT generations_output_placement_check,
    DROP COLUMN target_kind,
    DROP COLUMN caller_kind,
    DROP COLUMN stream_tokens,
    DROP COLUMN output_text,
    DROP COLUMN usage,
    ADD CONSTRAINT generations_modality_check CHECK (modality IN ('image', 'video')),
    ADD CONSTRAINT generations_output_placement_check
        CHECK (output_placement IN ('object_store', 'worker_local'));

-- Artifacts -------------------------------------------------------------------
ALTER TABLE artifacts
    DROP CONSTRAINT artifacts_placement_check,
    DROP COLUMN comfy_output_pointers,
    ADD COLUMN content bytea,
    ADD CONSTRAINT artifacts_placement_check
        CHECK (placement IN ('object_store', 'worker_local', 'remote_local')),
    ADD CONSTRAINT artifacts_remote_local_is_input
        CHECK (placement <> 'remote_local' OR direction = 'input');
-- Generation-scoped pool rejections -------------------------------------------
-- A pool may reject a lease before execution without consuming an Attempt.
-- Its exclusion is bounded by the finite pools registered for this Tenant and
-- disappears with either the Generation or Device Pool.
CREATE TABLE generation_pool_rejections (
    tenant_id uuid NOT NULL,
    generation_id uuid NOT NULL,
    pool_id uuid NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, generation_id, pool_id),
    FOREIGN KEY (tenant_id, generation_id)
        REFERENCES generations (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, pool_id)
        REFERENCES device_pools (tenant_id, id) ON DELETE CASCADE
);

ALTER TABLE generation_pool_rejections ENABLE ROW LEVEL SECURITY;
ALTER TABLE generation_pool_rejections FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON generation_pool_rejections
    FOR ALL TO gpq_app
    USING (tenant_id = gpq_current_tenant())
    WITH CHECK (tenant_id = gpq_current_tenant());
CREATE POLICY administration ON generation_pool_rejections
    FOR ALL TO gpq_admin
    USING (true)
    WITH CHECK (true);
GRANT SELECT, INSERT, UPDATE, DELETE ON generation_pool_rejections TO gpq_app;
GRANT ALL ON generation_pool_rejections TO gpq_admin;
