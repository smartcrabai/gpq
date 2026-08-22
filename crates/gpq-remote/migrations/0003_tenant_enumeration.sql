-- Background maintenance must enumerate Tenants.
--
-- The scheduler's fallback tick, the lease-expiry and execution-deadline
-- sweeps, Artifact expiry, cancellation propagation, and the startup
-- cancellation of synchronous Generations (ADR 0003, ADR 0008, ADR 0013) all
-- iterate every Tenant. Under the forced row-level security of ADR 0011 the
-- serving role sees no `tenants` row until `gpq.tenant_id` is already set, which
-- is exactly what those loops are trying to discover, so they silently did
-- nothing.
--
-- Like the credential lookups in `0002_credential_lookup`, this is a narrow
-- `SECURITY DEFINER` function owned by the administration role: it returns
-- Tenant identifiers only, never tenant-owned data, and every subsequent query
-- still runs inside a tenant-scoped transaction under the ordinary policies.

DO $$
BEGIN
    IF NOT pg_has_role(current_user, 'gpq_admin', 'MEMBER') THEN
        EXECUTE format('GRANT gpq_admin TO %I', current_user);
    END IF;
END
$$;

CREATE FUNCTION gpq_active_tenants() RETURNS SETOF uuid
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog, public
AS $$
    SELECT id FROM public.tenants WHERE deleted_at IS NULL ORDER BY id
$$;

ALTER FUNCTION gpq_active_tenants() OWNER TO gpq_admin;
REVOKE ALL ON FUNCTION gpq_active_tenants() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION gpq_active_tenants() TO gpq_app, gpq_admin;
