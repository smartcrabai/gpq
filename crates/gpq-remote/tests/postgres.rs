//! `PostgreSQL` integration suite: schema, RLS enforcement, and the queue's
//! invariants (leasing, result acceptance, heartbeats, lease expiry,
//! notifications, Artifact lifecycle, idempotency).
//!
//! `gpq-remote` is a library crate (`src/lib.rs`), so behavioral assertions
//! here call the real `gpq_remote::db::*` functions directly — including the
//! multi-step transaction sequences `scheduler.rs`/`expiry.rs` run — instead
//! of hand-copied SQL: no test in this file may assert against SQL text
//! defined here. Raw `sqlx::query`/`query_as` remain only for schema-level
//! concerns no `db::*` function owns (migrations, row-level security
//! policies, check constraints, `LISTEN/NOTIFY` triggers, unique
//! constraints), plus fixture setup and read-only state verification around
//! a retargeted call.
//!
//! Every test shares one `testcontainers`-managed `PostgreSQL` 18 container,
//! started lazily on first use via `support::Harness`; no external
//! `PostgreSQL` and no Docker configuration of any kind is required. The
//! container is removed again by `zzz_reap_shared_container`, named to sort
//! last so `--test-threads=1` runs it after every other test; a test that
//! happens to run after it simply starts a fresh container.

mod support;

use std::collections::BTreeSet;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use gpq_domain::{
    ArtifactId, AttemptId, DevicePoolId, GenerationId, RetryDecision, TenantId, WorkerId,
};
use gpq_remote::db::{artifacts, attempts, generations};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use support::{
    AttemptSpec, GenerationSpec, Harness, app_tx, content_hash, hmac_digest, insert_attempt,
    insert_device_pool, insert_generation, insert_input_artifact, insert_master_key,
    insert_output_artifact, insert_tenant, insert_worker, link_input_artifact, soft_delete_tenant,
    tenant_tx,
};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Migrations and row-level security (ADR 0011, ADR 0016)
// ---------------------------------------------------------------------------

/// ADR 0016: `gpq-remote migrate` applies the embedded migrations in order,
/// and re-running them against an already-migrated database is a no-op.
#[tokio::test]
async fn migrations_apply_in_order_and_reapplication_is_a_noop() -> anyhow::Result<()> {
    let harness = Harness::new().await?;

    let rows: Vec<(i64,)> = sqlx::query_as("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(harness.pool())
        .await?;
    let versions: Vec<i64> = rows.into_iter().map(|(version,)| version).collect();
    assert_eq!(
        versions,
        // 0004 makes `device_pools.free_slots` a generated column derived
        // from `claimed_slots`; 0005 indexes the execution-deadline sweep;
        // 0006 admits the mlx-dspark backend kind.
        vec![1, 2, 3, 4, 5, 6],
        "unexpected applied migration versions: {versions:?}"
    );

    sqlx::migrate!("./migrations").run(harness.pool()).await?;
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(harness.pool())
        .await?;
    // Derived from the set observed above, so adding a migration never
    // requires touching this assertion again.
    assert_eq!(
        usize::try_from(count).unwrap_or(usize::MAX),
        versions.len(),
        "re-running migrations must not duplicate rows"
    );

    harness.teardown().await
}

/// Migration 0006 must make the new backend kind persistable, not merely add
/// the enum value to the generated protocol bindings.
#[tokio::test]
async fn mlx_dspark_backend_kind_is_accepted_by_device_pools() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let tenant_id = Uuid::now_v7();
    let worker_id = Uuid::now_v7();
    let pool_id = Uuid::now_v7();
    insert_tenant(harness.pool(), tenant_id, "mlx-tenant").await?;
    insert_worker(
        harness.pool(),
        tenant_id,
        worker_id,
        "mlx-worker",
        &[0; 32],
        None,
    )
    .await?;
    sqlx::query(
        "INSERT INTO device_pools \
         (tenant_id, id, worker_id, pool_key, backend_kind, total_slots, claimed_slots) \
         VALUES ($1, $2, $3, $4, 'mlx_dspark', 1, 0)",
    )
    .bind(tenant_id)
    .bind(pool_id)
    .bind(worker_id)
    .bind("apple0")
    .execute(harness.pool())
    .await?;
    let (backend_kind,): (String,) =
        sqlx::query_as("SELECT backend_kind FROM device_pools WHERE id = $1")
            .bind(pool_id)
            .fetch_one(harness.pool())
            .await?;
    assert_eq!(backend_kind, "mlx_dspark");
    harness.teardown().await
}

/// Every table the migration's forced-RLS `DO` block plus `tenants` itself
/// covers (ADR 0011).
const TENANT_TABLES: &[&str] = &[
    "tenants",
    "tenant_master_keys",
    "workers",
    "device_pools",
    "pool_models",
    "model_versions",
    "model_aliases",
    "workflow_versions",
    "workflow_aliases",
    "generations",
    "attempts",
    "artifacts",
    "idempotency_keys",
    "generation_events",
];

/// ADR 0011, ADR 0016: every tenant-owned table (plus `tenants` itself) has
/// row-level security enabled AND forced, with exactly the `tenant_isolation`
/// and `administration` policies the migration creates.
#[tokio::test]
async fn every_tenant_owned_table_forces_rls_with_isolation_and_administration_policies()
-> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    for table in TENANT_TABLES {
        let (row_security, forced): (bool, bool) = sqlx::query_as(
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = $1",
        )
        .bind(table)
        .fetch_one(pool)
        .await?;
        assert!(
            row_security,
            "{table} does not have row-level security enabled"
        );
        assert!(
            forced,
            "{table} does not force row-level security on its owner"
        );

        let policies: Vec<(String,)> = sqlx::query_as(
            "SELECT policyname FROM pg_policies \
             WHERE schemaname = 'public' AND tablename = $1 ORDER BY policyname",
        )
        .bind(table)
        .fetch_all(pool)
        .await?;
        let names: BTreeSet<String> = policies.into_iter().map(|(name,)| name).collect();
        assert_eq!(
            names,
            BTreeSet::from(["administration".to_owned(), "tenant_isolation".to_owned()]),
            "{table} does not have exactly the tenant_isolation and administration policies"
        );
    }

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Tenant isolation (ADR 0001, ADR 0011)
// ---------------------------------------------------------------------------

/// ADR 0001, ADR 0011: under `gpq_app`, with `gpq.tenant_id` set, the
/// `tenant_isolation` policy scopes `generations`, `attempts`, and
/// `artifacts` reads to the current Tenant only.
#[tokio::test]
async fn tenant_isolation_scopes_reads_to_the_current_tenant() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant_a = Uuid::now_v7();
    let tenant_b = Uuid::now_v7();
    insert_tenant(pool, tenant_a, "tenant-a").await?;
    insert_tenant(pool, tenant_b, "tenant-b").await?;

    let (generation_a, worker_a, pool_a, attempt_a, artifact_a) =
        seed_full_chain(pool, tenant_a, "a").await?;
    let (_generation_b, _worker_b, _pool_b, _attempt_b, _artifact_b) =
        seed_full_chain(pool, tenant_b, "b").await?;
    let _ = (worker_a, pool_a);

    let mut tx = tenant_tx(pool, tenant_a).await?;
    let generations: Vec<(Uuid,)> = sqlx::query_as("SELECT id FROM generations")
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(
        generations,
        vec![(generation_a,)],
        "only tenant A's generations must be visible"
    );

    let attempts: Vec<(Uuid,)> = sqlx::query_as("SELECT id FROM attempts")
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(
        attempts,
        vec![(attempt_a,)],
        "only tenant A's attempts must be visible"
    );

    let artifacts: Vec<(Uuid,)> = sqlx::query_as("SELECT id FROM artifacts")
        .fetch_all(&mut *tx)
        .await?;
    assert_eq!(
        artifacts,
        vec![(artifact_a,)],
        "only tenant A's artifacts must be visible"
    );
    tx.rollback().await?;

    harness.teardown().await
}

/// ADR 0011: `WITH CHECK (tenant_id = gpq_current_tenant())` rejects an
/// insert whose `tenant_id` does not match the transaction's Tenant GUC.
#[tokio::test]
async fn tenant_isolation_rejects_a_cross_tenant_insert() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant_a = Uuid::now_v7();
    let tenant_b = Uuid::now_v7();
    insert_tenant(pool, tenant_a, "tenant-a").await?;
    insert_tenant(pool, tenant_b, "tenant-b").await?;

    let mut tx = tenant_tx(pool, tenant_a).await?;
    let result = sqlx::query(
        "INSERT INTO generations \
            (tenant_id, id, state, modality, caller_kind, target_kind, alias, version_sha256, \
             priority, execution_timeout, output_placement) \
         VALUES ($1, $2, 'queued', 'llm', 'durable', 'model', 'alias', $3, 5, interval '30 minutes', 'inline_relay')",
    )
    .bind(tenant_b)
    .bind(Uuid::now_v7())
    .bind(content_hash("cross-tenant"))
    .execute(&mut *tx)
    .await;
    assert!(
        result.is_err(),
        "inserting a row owned by another Tenant must be rejected by row-level security"
    );
    tx.rollback().await?;

    harness.teardown().await
}

/// ADR 0011: `gpq_current_tenant()` is `NULL` when `gpq.tenant_id` is unset,
/// and `tenant_id = NULL` is never true, so `gpq_app` sees nothing at all.
#[tokio::test]
async fn tenant_isolation_hides_everything_when_the_guc_is_unset() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;
    insert_generation(pool, &GenerationSpec::new(tenant, content_hash("model"))).await?;

    let mut tx = app_tx(pool).await?;
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM generations")
        .fetch_one(&mut *tx)
        .await?;
    tx.rollback().await?;
    assert_eq!(count, 0, "gpq_app with no tenant GUC must see zero rows");

    harness.teardown().await
}

/// Seeds one Tenant with a full generation -> attempt -> output artifact
/// chain, tagged by `label` so fixtures across Tenants stay distinguishable.
async fn seed_full_chain(
    pool: &PgPool,
    tenant: Uuid,
    label: &str,
) -> anyhow::Result<(Uuid, Uuid, Uuid, Uuid, Uuid)> {
    let worker = Uuid::now_v7();
    insert_worker(
        pool,
        tenant,
        worker,
        &format!("worker-{label}"),
        &hmac_digest(&Sha256::digest(label.as_bytes()).into(), label),
        None,
    )
    .await?;
    let device_pool = Uuid::now_v7();
    insert_device_pool(pool, tenant, device_pool, worker, "pool-0", 1, 0).await?;
    let generation = insert_generation(
        pool,
        &GenerationSpec {
            state: "running",
            attempt_count: 1,
            ..GenerationSpec::new(tenant, content_hash(&format!("model-{label}")))
        },
    )
    .await?;
    let attempt = insert_attempt(
        pool,
        &AttemptSpec::new(
            tenant,
            generation,
            worker,
            device_pool,
            Utc::now() + ChronoDuration::seconds(45),
        ),
    )
    .await?;
    let mut conn = pool.acquire().await?;
    let artifact = insert_output_artifact(&mut conn, tenant, generation, attempt, worker).await?;
    Ok((generation, worker, device_pool, attempt, artifact))
}

// ---------------------------------------------------------------------------
// Credential lookups (ADR 0009)
// ---------------------------------------------------------------------------

/// ADR 0009: `gpq_authenticate_master_key` resolves a live digest to its
/// Tenant and hides revoked, expired, and unknown digests, exercised as the
/// serving role `gpq_app` (not the owner) since a plain RLS-scoped `SELECT`
/// on `tenant_master_keys` cannot see this at all.
#[tokio::test]
async fn authenticate_master_key_honors_liveness_revocation_and_expiry() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let key = [3u8; 32];

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;

    let live = hmac_digest(&key, "live-master-key");
    insert_master_key(pool, tenant, Uuid::now_v7(), &live, None, None).await?;

    let revoked = hmac_digest(&key, "revoked-master-key");
    insert_master_key(
        pool,
        tenant,
        Uuid::now_v7(),
        &revoked,
        None,
        Some(Utc::now()),
    )
    .await?;

    let expired = hmac_digest(&key, "expired-master-key");
    insert_master_key(
        pool,
        tenant,
        Uuid::now_v7(),
        &expired,
        Some(Utc::now() - ChronoDuration::hours(1)),
        None,
    )
    .await?;

    let unknown = hmac_digest(&key, "never-issued");

    let mut tx = app_tx(pool).await?;
    let found: Option<Uuid> = sqlx::query_scalar("SELECT gpq_authenticate_master_key($1)")
        .bind(&live)
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(
        found,
        Some(tenant),
        "a live Master Key must resolve to its Tenant"
    );

    for (label, digest) in [
        ("revoked", &revoked),
        ("expired", &expired),
        ("unknown", &unknown),
    ] {
        let found: Option<Uuid> = sqlx::query_scalar("SELECT gpq_authenticate_master_key($1)")
            .bind(digest)
            .fetch_one(&mut *tx)
            .await?;
        assert_eq!(found, None, "{label} Master Key must not authenticate");
    }
    tx.rollback().await?;

    harness.teardown().await
}

/// ADR 0009: `gpq_authenticate_worker` resolves a live Worker Credential
/// digest to its `(tenant_id, worker_id)` and hides revoked and unknown
/// digests, exercised as `gpq_app`.
#[tokio::test]
async fn authenticate_worker_honors_liveness_and_revocation() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let key = [5u8; 32];

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;

    let live_worker = Uuid::now_v7();
    let live_digest = hmac_digest(&key, "live-worker-credential");
    insert_worker(pool, tenant, live_worker, "live-worker", &live_digest, None).await?;

    let revoked_worker = Uuid::now_v7();
    let revoked_digest = hmac_digest(&key, "revoked-worker-credential");
    insert_worker(
        pool,
        tenant,
        revoked_worker,
        "revoked-worker",
        &revoked_digest,
        Some(Utc::now()),
    )
    .await?;

    let unknown_digest = hmac_digest(&key, "never-enrolled");

    let mut tx = app_tx(pool).await?;
    let found: Option<(Uuid, Uuid)> =
        sqlx::query_as("SELECT tenant_id, worker_id FROM gpq_authenticate_worker($1)")
            .bind(&live_digest)
            .fetch_optional(&mut *tx)
            .await?;
    assert_eq!(
        found,
        Some((tenant, live_worker)),
        "a live Worker Credential must resolve to its Tenant and Worker"
    );

    for (label, digest) in [("revoked", &revoked_digest), ("unknown", &unknown_digest)] {
        let found: Option<(Uuid, Uuid)> =
            sqlx::query_as("SELECT tenant_id, worker_id FROM gpq_authenticate_worker($1)")
                .bind(digest)
                .fetch_optional(&mut *tx)
                .await?;
        assert_eq!(
            found, None,
            "{label} Worker Credential must not authenticate"
        );
    }
    tx.rollback().await?;

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Tenant enumeration (ADR 0011)
// ---------------------------------------------------------------------------

/// ADR 0011: `gpq_active_tenants()` enumerates non-deleted Tenants and hides
/// soft-deleted ones, exercised as `gpq_app`.
#[tokio::test]
async fn active_tenants_excludes_soft_deleted_tenants() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let active = Uuid::now_v7();
    let deleted = Uuid::now_v7();
    insert_tenant(pool, active, "active-tenant").await?;
    insert_tenant(pool, deleted, "deleted-tenant").await?;
    soft_delete_tenant(pool, deleted).await?;

    let mut tx = app_tx(pool).await?;
    let tenants: Vec<Uuid> = sqlx::query_scalar("SELECT * FROM gpq_active_tenants()")
        .fetch_all(&mut *tx)
        .await?;
    tx.rollback().await?;

    assert!(
        tenants.contains(&active),
        "the active tenant must be enumerated"
    );
    assert!(
        !tenants.contains(&deleted),
        "the soft-deleted tenant must be hidden"
    );

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Leasing (ADR 0013)
// ---------------------------------------------------------------------------

/// ADR 0013: two concurrent attempts to lease the same queued Generation
/// through the real claim path — `db::attempts::create` followed by
/// `db::generations::mark_running` in the same transaction, exactly what
/// `scheduler.rs::try_assign` runs before it commits — never both succeed.
/// `create`'s plain `FOR UPDATE` lock (no `SKIP LOCKED`, unlike the
/// scheduler's own candidate-selection query) blocks the loser until the
/// winner commits; the loser then finds the Generation already `Running`
/// and gets the documented `NotQueued` guard rather than a second Attempt.
/// Slot-capacity accounting (`db::workers::claim_slot`) is a separate
/// concern with its own coverage, so this test isolates the Generation-claim
/// guard from it.
#[tokio::test]
async fn concurrent_leasing_never_double_claims_a_queued_generation() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;
    let worker = Uuid::now_v7();
    insert_worker(
        pool,
        tenant,
        worker,
        "worker",
        &hmac_digest(&[8u8; 32], "worker"),
        None,
    )
    .await?;
    let device_pool = Uuid::now_v7();
    insert_device_pool(pool, tenant, device_pool, worker, "pool-0", 2, 0).await?;
    let generation =
        insert_generation(pool, &GenerationSpec::new(tenant, content_hash("model"))).await?;

    let tenant_id = TenantId::from_uuid(tenant);
    let generation_id = GenerationId::from_uuid(generation);
    let worker_id = WorkerId::from_uuid(worker);
    let pool_id = DevicePoolId::from_uuid(device_pool);
    let now = Utc::now();

    let claim_generation = |slot_key: &'static str| {
        let pool = pool.clone();
        async move {
            let mut tx = pool.begin().await?;
            let claimed = match attempts::create(
                &mut tx,
                tenant_id,
                generation_id,
                worker_id,
                pool_id,
                slot_key,
                gpq_domain::lease_expiry_from(now),
            )
            .await
            {
                Ok(attempt) => {
                    let transitioned =
                        generations::mark_running(&mut tx, tenant_id, generation_id, now).await?;
                    anyhow::ensure!(transitioned, "mark_running must succeed for the winner");
                    Some(attempt.id)
                }
                Err(attempts::CreateAttemptError::NotQueued) => None,
                Err(attempts::CreateAttemptError::MaxAttemptsReached { max }) => {
                    anyhow::bail!("unexpectedly hit the {max}-Attempt ceiling")
                }
                Err(attempts::CreateAttemptError::Database(error)) => return Err(error.into()),
            };
            tx.commit().await?;
            Ok(claimed)
        }
    };

    let (first, second) = tokio::try_join!(
        tokio::spawn(claim_generation("slot-a")),
        tokio::spawn(claim_generation("slot-b")),
    )?;
    let first: Option<Uuid> = first?;
    let second: Option<Uuid> = second?;

    let claims: Vec<Uuid> = [first, second].into_iter().flatten().collect();
    assert_eq!(
        claims.len(),
        1,
        "exactly one of the two concurrent create() calls must claim the Generation, got {claims:?}"
    );
    assert!(
        first.is_none() != second.is_none(),
        "the loser must see CreateAttemptError::NotQueued rather than also succeeding"
    );

    let live_attempt_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM attempts WHERE generation_id = $1 AND state IN ('leased', 'running')",
    )
    .bind(generation)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        live_attempt_count, 1,
        "the Generation must end up with exactly one live Attempt"
    );

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Result acceptance (ADR 0003): exercises db::generations::accept_result
// ---------------------------------------------------------------------------

/// Seeds one tenant/worker/pool/generation/attempt chain for the
/// result-acceptance tests, with the claimed Slot already reflected
/// (`free_slots = 0`) so accepting a result can prove it releases the Slot.
async fn seed_lease(
    pool: &PgPool,
    generation_state: &'static str,
    attempt_state: &'static str,
    lease_expires_at: DateTime<Utc>,
) -> anyhow::Result<(Uuid, Uuid, Uuid, Uuid)> {
    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;
    let worker = Uuid::now_v7();
    insert_worker(
        pool,
        tenant,
        worker,
        "worker",
        &hmac_digest(&[9u8; 32], "worker-secret"),
        None,
    )
    .await?;
    let device_pool = Uuid::now_v7();
    insert_device_pool(pool, tenant, device_pool, worker, "pool-0", 1, 0).await?;
    let generation = insert_generation(
        pool,
        &GenerationSpec {
            state: generation_state,
            attempt_count: 1,
            ..GenerationSpec::new(tenant, content_hash("model"))
        },
    )
    .await?;
    let attempt = insert_attempt(
        pool,
        &AttemptSpec {
            state: attempt_state,
            ..AttemptSpec::new(tenant, generation, worker, device_pool, lease_expires_at)
        },
    )
    .await?;
    Ok((tenant, worker, generation, attempt))
}

/// ADR 0003, ADR 0005: the first result settles both the Attempt and the
/// Generation as succeeded and releases the Slot back to the Pool.
#[tokio::test]
async fn accept_result_settles_the_first_result_and_releases_the_slot() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let now = Utc::now();

    let (tenant, worker, generation, attempt) =
        seed_lease(pool, "running", "leased", now + ChronoDuration::seconds(45)).await?;

    let mut tx = pool.begin().await?;
    let outcome = generations::accept_result(
        &mut tx,
        TenantId::from_uuid(tenant),
        AttemptId::from_uuid(attempt),
        WorkerId::from_uuid(worker),
        "hello world",
        Some((12, 34, 46)),
        now,
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        outcome,
        generations::AcceptOutcome::Accepted(GenerationId::from_uuid(generation))
    );

    let (state, accepted_attempt_id, output_text, usage): (
        String,
        Option<Uuid>,
        String,
        Option<serde_json::Value>,
    ) = sqlx::query_as(
        "SELECT state, accepted_attempt_id, output_text, usage FROM generations WHERE id = $1",
    )
    .bind(generation)
    .fetch_one(pool)
    .await?;
    assert_eq!(state, "succeeded");
    assert_eq!(accepted_attempt_id, Some(attempt));
    assert_eq!(output_text, "hello world");
    assert_eq!(
        usage,
        Some(serde_json::json!({
            "prompt_tokens": 12,
            "completion_tokens": 34,
            "total_tokens": 46,
        })),
        "accept_result must persist the reported token usage"
    );

    let (free_slots,): (i32,) =
        sqlx::query_as("SELECT free_slots FROM device_pools WHERE worker_id = $1")
            .bind(worker)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        free_slots, 1,
        "the claimed Slot must be released back to the Pool"
    );

    harness.teardown().await
}

/// ADR 0003: a second result for a Generation that already has an Accepted
/// Result is rejected, and `accepted_attempt_id` never changes.
#[tokio::test]
async fn accept_result_rejects_a_second_result_once_accepted() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let now = Utc::now();

    let (tenant, worker, generation, attempt) =
        seed_lease(pool, "running", "leased", now + ChronoDuration::seconds(45)).await?;

    let mut tx = pool.begin().await?;
    let first = generations::accept_result(
        &mut tx,
        TenantId::from_uuid(tenant),
        AttemptId::from_uuid(attempt),
        WorkerId::from_uuid(worker),
        "first",
        None,
        now,
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        first,
        generations::AcceptOutcome::Accepted(GenerationId::from_uuid(generation))
    );

    // A second Attempt for the same Generation (a duplicate late retry
    // result) must not overwrite the already-Accepted Result.
    let device_pool: Uuid = sqlx::query_scalar("SELECT id FROM device_pools WHERE worker_id = $1")
        .bind(worker)
        .fetch_one(pool)
        .await?;
    let second_attempt = insert_attempt(
        pool,
        &AttemptSpec {
            attempt_number: 2,
            ..AttemptSpec::new(
                tenant,
                generation,
                worker,
                device_pool,
                now + ChronoDuration::seconds(45),
            )
        },
    )
    .await?;

    let mut tx = pool.begin().await?;
    let second = generations::accept_result(
        &mut tx,
        TenantId::from_uuid(tenant),
        AttemptId::from_uuid(second_attempt),
        WorkerId::from_uuid(worker),
        "second",
        None,
        now,
    )
    .await?;
    tx.commit().await?;
    assert_eq!(second, generations::AcceptOutcome::AlreadyAccepted);

    let (accepted_attempt_id,): (Option<Uuid>,) =
        sqlx::query_as("SELECT accepted_attempt_id FROM generations WHERE id = $1")
            .bind(generation)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        accepted_attempt_id,
        Some(attempt),
        "accepted_attempt_id must never change once set"
    );

    harness.teardown().await
}

/// ADR 0003: a result committed under an expired lease is rejected as
/// `StaleLease` and mutates nothing.
#[tokio::test]
async fn accept_result_rejects_a_result_under_an_expired_lease() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let now = Utc::now();

    let (tenant, worker, generation, attempt) =
        seed_lease(pool, "running", "leased", now - ChronoDuration::minutes(1)).await?;

    let mut tx = pool.begin().await?;
    let outcome = generations::accept_result(
        &mut tx,
        TenantId::from_uuid(tenant),
        AttemptId::from_uuid(attempt),
        WorkerId::from_uuid(worker),
        "too late",
        None,
        now,
    )
    .await?;
    tx.commit().await?;
    assert_eq!(outcome, generations::AcceptOutcome::StaleLease);

    let (state,): (String,) = sqlx::query_as("SELECT state FROM generations WHERE id = $1")
        .bind(generation)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        state, "running",
        "a stale-lease result must not mutate the Generation"
    );

    harness.teardown().await
}

/// ADR 0003: a result for a Generation that already reached a terminal
/// state (e.g. it was cancelled) is rejected as `Terminal`.
#[tokio::test]
async fn accept_result_rejects_a_result_for_a_terminal_generation() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let now = Utc::now();

    let (tenant, worker, generation, attempt) = seed_lease(
        pool,
        "cancelled",
        "leased",
        now + ChronoDuration::seconds(45),
    )
    .await?;

    let mut tx = pool.begin().await?;
    let outcome = generations::accept_result(
        &mut tx,
        TenantId::from_uuid(tenant),
        AttemptId::from_uuid(attempt),
        WorkerId::from_uuid(worker),
        "too late",
        None,
        now,
    )
    .await?;
    tx.commit().await?;
    assert_eq!(outcome, generations::AcceptOutcome::Terminal);

    let (accepted_attempt_id,): (Option<Uuid>,) =
        sqlx::query_as("SELECT accepted_attempt_id FROM generations WHERE id = $1")
            .bind(generation)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        accepted_attempt_id, None,
        "a terminal Generation must not accept a result"
    );

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Heartbeats and lease expiry (ADR 0003): exercises
// db::attempts::{heartbeat, expired_leases, request_cancel, record_lease_expiry}
// ---------------------------------------------------------------------------

/// ADR 0003: a heartbeat renews `lease_expires_at` to `now + LEASE_TTL`, only
/// for the named Worker's own live (`leased`/`running`) Attempts — a
/// different Worker's Attempt and an already-terminal Attempt are untouched.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one linear fixture-then-assert narrative: two Workers, four Attempts in               different states, and the renewal outcome of each; splitting it would hide               which rows the single heartbeat call was expected to touch"
)]
async fn heartbeat_renews_only_the_named_workers_live_attempts() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let (db_now,): (DateTime<Utc>,) = sqlx::query_as("SELECT now()").fetch_one(pool).await?;

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;

    let worker_x = Uuid::now_v7();
    insert_worker(
        pool,
        tenant,
        worker_x,
        "worker-x",
        &hmac_digest(&[1u8; 32], "x"),
        None,
    )
    .await?;
    let pool_x = Uuid::now_v7();
    insert_device_pool(pool, tenant, pool_x, worker_x, "pool-x", 1, 0).await?;

    let worker_y = Uuid::now_v7();
    insert_worker(
        pool,
        tenant,
        worker_y,
        "worker-y",
        &hmac_digest(&[2u8; 32], "y"),
        None,
    )
    .await?;
    let pool_y = Uuid::now_v7();
    insert_device_pool(pool, tenant, pool_y, worker_y, "pool-y", 1, 0).await?;

    let about_to_lapse = db_now + ChronoDuration::seconds(5);
    let live_generation = insert_generation(
        pool,
        &GenerationSpec {
            state: "running",
            attempt_count: 1,
            ..GenerationSpec::new(tenant, content_hash("m1"))
        },
    )
    .await?;
    let live_attempt = insert_attempt(
        pool,
        &AttemptSpec::new(tenant, live_generation, worker_x, pool_x, about_to_lapse),
    )
    .await?;

    let other_worker_generation = insert_generation(
        pool,
        &GenerationSpec {
            state: "running",
            attempt_count: 1,
            ..GenerationSpec::new(tenant, content_hash("m2"))
        },
    )
    .await?;
    let other_worker_attempt = insert_attempt(
        pool,
        &AttemptSpec::new(
            tenant,
            other_worker_generation,
            worker_y,
            pool_y,
            about_to_lapse,
        ),
    )
    .await?;

    let terminal_generation = insert_generation(
        pool,
        &GenerationSpec {
            state: "running",
            attempt_count: 1,
            ..GenerationSpec::new(tenant, content_hash("m3"))
        },
    )
    .await?;
    let terminal_attempt = insert_attempt(
        pool,
        &AttemptSpec {
            state: "succeeded",
            ..AttemptSpec::new(
                tenant,
                terminal_generation,
                worker_x,
                pool_x,
                about_to_lapse,
            )
        },
    )
    .await?;
    sqlx::query(
        "UPDATE generations SET state = 'succeeded', accepted_attempt_id = $2 WHERE id = $1",
    )
    .bind(terminal_generation)
    .bind(terminal_attempt)
    .execute(pool)
    .await?;

    let not_renewed = {
        let mut tx = pool.begin().await?;
        let not_renewed = attempts::heartbeat(
            &mut tx,
            TenantId::from_uuid(tenant),
            WorkerId::from_uuid(worker_x),
            &[
                AttemptId::from_uuid(live_attempt),
                AttemptId::from_uuid(terminal_attempt),
            ],
            db_now,
        )
        .await?;
        tx.commit().await?;
        not_renewed
    };
    assert_eq!(
        not_renewed,
        vec![AttemptId::from_uuid(terminal_attempt)],
        "only the terminal Attempt should be reported as not renewed"
    );

    let (renewed_expiry,): (DateTime<Utc>,) =
        sqlx::query_as("SELECT lease_expires_at FROM attempts WHERE id = $1")
            .bind(live_attempt)
            .fetch_one(pool)
            .await?;
    let expected = db_now + ChronoDuration::from_std(gpq_domain::LEASE_TTL)?;
    assert!(
        (renewed_expiry - expected).num_milliseconds().abs() < 1000,
        "renewed lease should expire ~{expected:?} past now, got {renewed_expiry:?}"
    );

    let (untouched_expiry,): (DateTime<Utc>,) =
        sqlx::query_as("SELECT lease_expires_at FROM attempts WHERE id = $1")
            .bind(other_worker_attempt)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        untouched_expiry, about_to_lapse,
        "another Worker's Attempt must not be renewed by this heartbeat"
    );

    harness.teardown().await
}

/// ADR 0003, ADR 0013: the expiry sweep's `FOR UPDATE SKIP LOCKED` selection
/// finds only lapsed leases (not a live one), and applies the retry policy —
/// requeue below three Attempts, fail once three have been used.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "walks the whole ADR 0003 retry ladder in one timeline: a lapsed lease below               the Attempt budget requeues, the third lapse fails the Generation; splitting               the timeline would break the causal order the assertions depend on"
)]
async fn lease_expiry_sweep_selects_lapsed_leases_and_applies_the_retry_policy()
-> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();
    let (db_now,): (DateTime<Utc>,) = sqlx::query_as("SELECT now()").fetch_one(pool).await?;

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;
    let worker = Uuid::now_v7();
    insert_worker(
        pool,
        tenant,
        worker,
        "worker",
        &hmac_digest(&[4u8; 32], "worker"),
        None,
    )
    .await?;
    let device_pool = Uuid::now_v7();
    insert_device_pool(pool, tenant, device_pool, worker, "pool-0", 3, 0).await?;

    let lapsed = db_now - ChronoDuration::seconds(1);
    let live = db_now + ChronoDuration::seconds(45);

    // Below the retry ceiling: requeues.
    let requeue_generation = insert_generation(
        pool,
        &GenerationSpec {
            state: "running",
            attempt_count: 1,
            ..GenerationSpec::new(tenant, content_hash("g1"))
        },
    )
    .await?;
    let requeue_attempt = insert_attempt(
        pool,
        &AttemptSpec::new(tenant, requeue_generation, worker, device_pool, lapsed),
    )
    .await?;

    // At the retry ceiling: fails.
    let fail_generation = insert_generation(
        pool,
        &GenerationSpec {
            state: "running",
            attempt_count: 3,
            ..GenerationSpec::new(tenant, content_hash("g2"))
        },
    )
    .await?;
    let fail_attempt = insert_attempt(
        pool,
        &AttemptSpec {
            attempt_number: 3,
            ..AttemptSpec::new(tenant, fail_generation, worker, device_pool, lapsed)
        },
    )
    .await?;

    // Control: a live lease must not be selected by the sweep.
    let live_generation = insert_generation(
        pool,
        &GenerationSpec {
            state: "running",
            attempt_count: 1,
            ..GenerationSpec::new(tenant, content_hash("g3"))
        },
    )
    .await?;
    let live_attempt = insert_attempt(
        pool,
        &AttemptSpec::new(tenant, live_generation, worker, device_pool, live),
    )
    .await?;

    let tenant_id = TenantId::from_uuid(tenant);
    let mut tx = pool.begin().await?;
    let selected = attempts::expired_leases(&mut tx, tenant_id, db_now, 10).await?;
    let selected_ids: BTreeSet<Uuid> = selected.iter().map(|attempt| attempt.id).collect();
    assert_eq!(
        selected_ids,
        BTreeSet::from([requeue_attempt, fail_attempt]),
        "the sweep must select exactly the lapsed leases, not the live one"
    );
    assert!(!selected_ids.contains(&live_attempt));

    // Reproduces `expiry.rs::expire_leases`'s per-Attempt order: cooperative
    // cancellation is requested before the retry policy settles the Attempt.
    let mut requeue_decision = None;
    let mut fail_decision = None;
    for attempt in &selected {
        attempts::request_cancel(&mut tx, tenant_id, attempt.attempt_id(), db_now).await?;
        let decision =
            attempts::record_lease_expiry(&mut tx, tenant_id, attempt.attempt_id(), db_now).await?;
        if attempt.id == requeue_attempt {
            requeue_decision = decision;
        } else if attempt.id == fail_attempt {
            fail_decision = decision;
        }
    }
    tx.commit().await?;

    assert_eq!(
        requeue_decision,
        Some((
            GenerationId::from_uuid(requeue_generation),
            RetryDecision::Requeue
        ))
    );
    assert_eq!(
        fail_decision,
        Some((
            GenerationId::from_uuid(fail_generation),
            RetryDecision::Fail
        ))
    );

    let (requeue_state,): (String,) = sqlx::query_as("SELECT state FROM generations WHERE id = $1")
        .bind(requeue_generation)
        .fetch_one(pool)
        .await?;
    assert_eq!(requeue_state, "queued");

    let (fail_state, failure_kind): (String, Option<String>) =
        sqlx::query_as("SELECT state, failure_kind FROM generations WHERE id = $1")
            .bind(fail_generation)
            .fetch_one(pool)
            .await?;
    assert_eq!(fail_state, "failed");
    assert_eq!(failure_kind, Some("lease_expired".to_owned()));

    let (free_slots,): (i32,) = sqlx::query_as("SELECT free_slots FROM device_pools WHERE id = $1")
        .bind(device_pool)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        free_slots, 2,
        "both lapsed Attempts' Slots must be released"
    );

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Check constraints and state guards (ADR 0017)
// ---------------------------------------------------------------------------

/// ADR 0017: illegal state text, out-of-range priority, and terminal states
/// missing their required companion column, and a malformed content hash
/// are all rejected by `generations`' own check constraints.
#[tokio::test]
async fn generations_check_constraints_reject_illegal_state_and_range_values() -> anyhow::Result<()>
{
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;

    let cases: Vec<(&str, anyhow::Result<Uuid>)> = vec![
        (
            "illegal generations.state",
            insert_generation(
                pool,
                &GenerationSpec {
                    state: "bogus",
                    ..GenerationSpec::new(tenant, content_hash("m1"))
                },
            )
            .await,
        ),
        (
            "priority above 9",
            insert_generation(
                pool,
                &GenerationSpec {
                    priority: 10,
                    ..GenerationSpec::new(tenant, content_hash("m2"))
                },
            )
            .await,
        ),
        (
            "priority below 0",
            insert_generation(
                pool,
                &GenerationSpec {
                    priority: -1,
                    ..GenerationSpec::new(tenant, content_hash("m3"))
                },
            )
            .await,
        ),
        (
            "succeeded without accepted_attempt_id",
            insert_generation(
                pool,
                &GenerationSpec {
                    state: "succeeded",
                    accepted_attempt_id: None,
                    ..GenerationSpec::new(tenant, content_hash("m4"))
                },
            )
            .await,
        ),
        (
            "failed without failure_kind",
            insert_generation(
                pool,
                &GenerationSpec {
                    state: "failed",
                    failure_kind: None,
                    ..GenerationSpec::new(tenant, content_hash("m5"))
                },
            )
            .await,
        ),
        (
            "non-hex version_sha256",
            insert_generation(
                pool,
                &GenerationSpec {
                    version_sha256: "not-a-valid-hash".to_owned(),
                    ..GenerationSpec::new(tenant, content_hash("m6"))
                },
            )
            .await,
        ),
    ];
    for (label, result) in cases {
        assert!(
            result.is_err(),
            "{label} must be rejected by a check constraint"
        );
    }

    harness.teardown().await
}

/// ADR 0017: `attempt_number` outside `1..=3` is rejected by `attempts`'
/// own check constraint.
#[tokio::test]
async fn attempts_check_constraint_rejects_out_of_range_attempt_number() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;
    let worker = Uuid::now_v7();
    insert_worker(
        pool,
        tenant,
        worker,
        "worker",
        &hmac_digest(&[6u8; 32], "worker"),
        None,
    )
    .await?;
    let device_pool = Uuid::now_v7();
    insert_device_pool(pool, tenant, device_pool, worker, "pool-0", 1, 1).await?;
    let generation =
        insert_generation(pool, &GenerationSpec::new(tenant, content_hash("m7"))).await?;

    let cases: Vec<(&str, anyhow::Result<Uuid>)> = vec![
        (
            "attempt_number above 3",
            insert_attempt(
                pool,
                &AttemptSpec {
                    attempt_number: 4,
                    ..AttemptSpec::new(tenant, generation, worker, device_pool, Utc::now())
                },
            )
            .await,
        ),
        (
            "attempt_number below 1",
            insert_attempt(
                pool,
                &AttemptSpec {
                    attempt_number: 0,
                    ..AttemptSpec::new(tenant, generation, worker, device_pool, Utc::now())
                },
            )
            .await,
        ),
    ];
    for (label, result) in cases {
        assert!(
            result.is_err(),
            "{label} must be rejected by a check constraint"
        );
    }

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// LISTEN/NOTIFY (ADR 0013)
// ---------------------------------------------------------------------------

/// ADR 0013: enqueueing and re-queuing a Generation both notify `gpq_queue`
/// with the Tenant id; a transition away from `queued` does not.
#[tokio::test]
async fn queue_notification_fires_on_enqueue_and_requeue_but_not_other_transitions()
-> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;

    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
    listener.listen("gpq_queue").await?;

    let generation =
        insert_generation(pool, &GenerationSpec::new(tenant, content_hash("model"))).await?;

    let notification = tokio::time::timeout(StdDuration::from_secs(5), listener.recv())
        .await
        .map_err(|_| anyhow::anyhow!("no notification for the initial enqueue"))??;
    assert_eq!(
        notification.payload(),
        tenant.to_string(),
        "notification must carry the tenant id"
    );

    sqlx::query("UPDATE generations SET state = 'running' WHERE id = $1")
        .bind(generation)
        .execute(pool)
        .await?;
    let silent = tokio::time::timeout(StdDuration::from_millis(500), listener.recv()).await;
    assert!(
        silent.is_err(),
        "a transition away from queued must not notify"
    );

    sqlx::query("UPDATE generations SET state = 'queued' WHERE id = $1")
        .bind(generation)
        .execute(pool)
        .await?;
    let notification = tokio::time::timeout(StdDuration::from_secs(5), listener.recv())
        .await
        .map_err(|_| anyhow::anyhow!("no notification for the requeue"))??;
    assert_eq!(
        notification.payload(),
        tenant.to_string(),
        "the requeue notification must also carry the tenant id"
    );

    // `PgListener` holds a checked-out pool connection for its whole
    // lifetime; `Harness::teardown` closes the pool, which waits for every
    // connection to return, so the listener must be dropped first.
    drop(listener);
    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Artifact lifecycle (ADR 0008)
// ---------------------------------------------------------------------------

/// ADR 0008: `available -> delivering -> consumed` succeeds end to end.
#[tokio::test]
async fn artifact_lifecycle_available_to_delivering_to_consumed() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let (tenant, worker, generation, attempt) = seed_lease(
        pool,
        "running",
        "leased",
        Utc::now() + ChronoDuration::seconds(45),
    )
    .await?;
    let mut conn = pool.acquire().await?;
    let artifact = insert_output_artifact(&mut conn, tenant, generation, attempt, worker).await?;

    let began = artifacts::begin_delivery(
        &mut conn,
        TenantId::from_uuid(tenant),
        ArtifactId::from_uuid(artifact),
    )
    .await?;
    assert!(began, "available -> delivering must succeed");

    let consumed = artifacts::set_state(
        &mut conn,
        TenantId::from_uuid(tenant),
        ArtifactId::from_uuid(artifact),
        gpq_domain::ArtifactState::Consumed,
    )
    .await?;
    assert_eq!(consumed.state, gpq_domain::ArtifactState::Consumed);
    drop(conn);

    let (state, terminated_at): (String, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT state, terminated_at FROM artifacts WHERE id = $1")
            .bind(artifact)
            .fetch_one(pool)
            .await?;
    assert_eq!(state, "consumed");
    assert!(
        terminated_at.is_some(),
        "a terminal Artifact must record terminated_at"
    );

    harness.teardown().await
}

/// ADR 0008: a second concurrent `available -> delivering` transition loses
/// — the case behind the download route's `409 Conflict`.
#[tokio::test]
async fn artifact_second_delivery_attempt_loses_the_conflict() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let (tenant, worker, generation, attempt) = seed_lease(
        pool,
        "running",
        "leased",
        Utc::now() + ChronoDuration::seconds(45),
    )
    .await?;
    let mut conn = pool.acquire().await?;
    let artifact = insert_output_artifact(&mut conn, tenant, generation, attempt, worker).await?;

    let tenant_id = TenantId::from_uuid(tenant);
    let artifact_id = ArtifactId::from_uuid(artifact);
    let first = artifacts::begin_delivery(&mut conn, tenant_id, artifact_id).await?;
    assert!(first, "the first delivery attempt must win");

    let second = artifacts::begin_delivery(&mut conn, tenant_id, artifact_id).await?;
    assert!(
        !second,
        "a second concurrent delivery attempt must lose the conflict"
    );
    drop(conn);

    harness.teardown().await
}

/// ADR 0008: an output Artifact expires exactly one hour after
/// `available_at`.
#[tokio::test]
async fn artifact_output_expires_one_hour_after_completion() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let (tenant, worker, generation, attempt) = seed_lease(
        pool,
        "running",
        "leased",
        Utc::now() + ChronoDuration::seconds(45),
    )
    .await?;
    let mut conn = pool.acquire().await?;
    let artifact = insert_output_artifact(&mut conn, tenant, generation, attempt, worker).await?;
    drop(conn);

    let (available_at, expires_at): (DateTime<Utc>, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT available_at, expires_at FROM artifacts WHERE id = $1")
            .bind(artifact)
            .fetch_one(pool)
            .await?;
    let Some(expires_at) = expires_at else {
        anyhow::bail!("an available output Artifact must have an expires_at");
    };
    let delta = (expires_at - available_at).num_seconds();
    assert_eq!(
        delta, 3600,
        "an output Artifact must expire exactly one hour after completion"
    );

    harness.teardown().await
}

/// ADR 0008: deleting a Generation's input Artifacts (its terminal
/// transition) returns exactly the deleted rows.
#[tokio::test]
async fn delete_inputs_for_generation_returns_the_deleted_rows() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;
    let generation =
        insert_generation(pool, &GenerationSpec::new(tenant, content_hash("model"))).await?;

    let input_a = insert_input_artifact(pool, tenant).await?;
    let input_b = insert_input_artifact(pool, tenant).await?;
    link_input_artifact(pool, tenant, input_a, generation).await?;
    link_input_artifact(pool, tenant, input_b, generation).await?;

    let mut conn = pool.acquire().await?;
    let deleted = artifacts::delete_inputs_for_generation(
        &mut conn,
        TenantId::from_uuid(tenant),
        GenerationId::from_uuid(generation),
    )
    .await?;
    let deleted: BTreeSet<Uuid> = deleted.into_iter().map(|row| row.id.as_uuid()).collect();
    assert_eq!(deleted, BTreeSet::from([input_a, input_b]));
    drop(conn);

    let (remaining,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM artifacts WHERE generation_id = $1")
            .bind(generation)
            .fetch_one(pool)
            .await?;
    assert_eq!(remaining, 0, "the deleted input Artifacts must be gone");

    harness.teardown().await
}

// ---------------------------------------------------------------------------
// Idempotency (ADR 0006)
// ---------------------------------------------------------------------------

/// ADR 0006: two admission requests with the same `(tenant_id, key)` conflict
/// at the database, which is exactly what lets admission replay the
/// original Generation instead of duplicating work.
#[tokio::test]
async fn idempotency_key_insert_conflicts_on_duplicate_tenant_and_key() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let pool = harness.pool();

    let tenant = Uuid::now_v7();
    insert_tenant(pool, tenant, "tenant").await?;
    let generation =
        insert_generation(pool, &GenerationSpec::new(tenant, content_hash("model"))).await?;
    let other_generation =
        insert_generation(pool, &GenerationSpec::new(tenant, content_hash("model"))).await?;

    let key = "client-supplied-key";
    let digest = content_hash("request-body").into_bytes();

    sqlx::query(
        "INSERT INTO idempotency_keys (tenant_id, key, request_digest, generation_id) VALUES ($1, $2, $3, $4)",
    )
    .bind(tenant)
    .bind(key)
    .bind(&digest)
    .bind(generation)
    .execute(pool)
    .await?;

    let conflict = sqlx::query(
        "INSERT INTO idempotency_keys (tenant_id, key, request_digest, generation_id) VALUES ($1, $2, $3, $4)",
    )
    .bind(tenant)
    .bind(key)
    .bind(&digest)
    .bind(other_generation)
    .execute(pool)
    .await;
    assert!(
        conflict.is_err(),
        "a duplicate (tenant_id, key) insert must conflict so admission can replay the original Generation"
    );

    let (stored,): (Uuid,) = sqlx::query_as(
        "SELECT generation_id FROM idempotency_keys WHERE tenant_id = $1 AND key = $2",
    )
    .bind(tenant)
    .bind(key)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        stored, generation,
        "the original Generation must remain the one replay resolves to"
    );

    harness.teardown().await
}

/// Removes the shared container once the suite is done with it.
///
/// `testcontainers` cleans up in `ContainerAsync::drop`, which never runs for
/// a container shared through a `static`, so the suite hands it back
/// explicitly here instead of leaving it to the daemon.
#[tokio::test]
async fn zzz_reap_shared_container() -> anyhow::Result<()> {
    support::reap_shared_container().await
}
