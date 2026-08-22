-- Credential authentication happens before the Tenant is known.
--
-- ADR 0011 gives every tenant-owned table forced row-level security and runs
-- serving under `gpq_app`, which has no `BYPASSRLS`. That makes the two
-- pre-authentication lookups impossible under the tenant policies: the Tenant
-- Master Key and the Worker Credential are presented as bare secrets, and their
-- owning Tenant is exactly what the lookup has to discover.
--
-- These two `SECURITY DEFINER` functions are therefore the only cross-tenant
-- reads the serving role can perform. Each takes a keyed digest (ADR 0009 never
-- stores the secret itself), returns identifiers only, and never exposes row
-- contents, so a leaked digest reveals nothing a forged bearer token would not
-- already have proven. ADR 0016 uses forward fixes, so this replaces nothing.

-- `SECURITY DEFINER` executes as the function owner, and forced RLS applies to
-- the table owner too, so the owner must be the administration role whose
-- policy is unconditional.
DO $$
BEGIN
    IF NOT pg_has_role(current_user, 'gpq_admin', 'MEMBER') THEN
        EXECUTE format('GRANT gpq_admin TO %I', current_user);
    END IF;
END
$$;

CREATE FUNCTION gpq_authenticate_master_key(digest bytea) RETURNS uuid
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog, public
AS $$
    SELECT tenant_id
    FROM public.tenant_master_keys
    WHERE key_hash = digest
      AND revoked_at IS NULL
      AND (expires_at IS NULL OR expires_at > now())
    LIMIT 1
$$;

CREATE FUNCTION gpq_authenticate_worker(digest bytea)
    RETURNS TABLE (tenant_id uuid, worker_id uuid)
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog, public
AS $$
    SELECT tenant_id, id
    FROM public.workers
    WHERE credential_hash = digest
      AND revoked_at IS NULL
    LIMIT 1
$$;

ALTER FUNCTION gpq_authenticate_master_key(bytea) OWNER TO gpq_admin;
ALTER FUNCTION gpq_authenticate_worker(bytea) OWNER TO gpq_admin;

REVOKE ALL ON FUNCTION gpq_authenticate_master_key(bytea) FROM PUBLIC;
REVOKE ALL ON FUNCTION gpq_authenticate_worker(bytea) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION gpq_authenticate_master_key(bytea) TO gpq_app, gpq_admin;
GRANT EXECUTE ON FUNCTION gpq_authenticate_worker(bytea) TO gpq_app, gpq_admin;

-- Enrollment inserts a Worker row while authenticated only by the Tenant Master
-- Key, and the Worker's own Tenant is known by then, so it stays under the
-- tenant policy. Nothing else needs a definer-rights path.
