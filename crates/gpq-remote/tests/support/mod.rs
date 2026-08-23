//! Shared `PostgreSQL` integration test scaffolding.
//!
//! [`Harness`] shares one `testcontainers`-managed `PostgreSQL` 18 container
//! for the whole `postgres` test binary (started lazily on first use),
//! creates a throwaway database per test run inside it, applies the crate's
//! real migrations, and drops the database again on [`Harness::teardown`].
//!
//! `gpq-remote` is a binary crate, so these tests cannot import its modules
//! (`crate::db::*`); every fixture and reproduced query below operates on raw
//! SQL against the migrated schema instead, matching the shape of the real
//! queries in `src/db/*.rs`.

#[path = "../shared/container.rs"]
mod shared_container;

pub use shared_container::reap_shared_container;

use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use shared_container::maintenance_url;
use sqlx::PgConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgPool, Postgres, Transaction};
use url::Url;
use uuid::Uuid;

/// A throwaway `PostgreSQL` database, migrated and ready to test against.
pub struct Harness {
    /// The shared container's maintenance connection string; used only to
    /// create and drop the per-run database.
    admin_url: String,
    /// The unique per-run database name (`gpq_test_<uuid-simple>`).
    db_name: String,
    pool: PgPool,
}

impl Harness {
    /// Creates a fresh, migrated database inside the shared `testcontainers`
    /// `PostgreSQL` 18 container for this test binary, starting the
    /// container first if no other test has yet.
    ///
    /// # Errors
    /// Returns an error if the container cannot be started, or the database
    /// cannot be created, connected to, or migrated.
    pub async fn new() -> anyhow::Result<Self> {
        let maintenance_url = maintenance_url().await?;
        let db_name = format!("gpq_test_{}", Uuid::now_v7().simple());

        let mut admin_conn = PgConnection::connect(&maintenance_url).await?;
        let create_sql = format!(r#"CREATE DATABASE "{db_name}""#);
        sqlx::query(sqlx::AssertSqlSafe(create_sql))
            .execute(&mut admin_conn)
            .await?;
        admin_conn.close().await?;

        let mut db_url = Url::parse(&maintenance_url)?;
        db_url.set_path(&format!("/{db_name}"));

        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(db_url.as_str())
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self {
            admin_url: maintenance_url,
            db_name,
            pool,
        })
    }

    /// The migrated pool, connected as the schema owner (a superuser in the
    /// shared test container, so it bypasses row-level security entirely —
    /// tests that must observe RLS enforcement use [`app_tx`] or
    /// [`tenant_tx`] instead).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Closes the pool and drops the per-run database. Call this as the last
    /// step of every test; an early `assert!` failure before it runs leaves a
    /// harmlessly named orphan database behind (each run picks a fresh
    /// `uuid`, so orphans never collide with a later run).
    ///
    /// # Errors
    /// Returns an error if the drop connection or statement fails.
    pub async fn teardown(self) -> anyhow::Result<()> {
        self.pool.close().await;
        let mut admin_conn = PgConnection::connect(&self.admin_url).await?;
        let drop_sql = format!(r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#, self.db_name);
        sqlx::query(sqlx::AssertSqlSafe(drop_sql))
            .execute(&mut admin_conn)
            .await?;
        admin_conn.close().await?;
        Ok(())
    }
}

/// Opens a transaction under the serving role `gpq_app`, with no Tenant GUC
/// set — used for the definer-rights credential and enumeration functions,
/// which run before a Tenant is known (ADR 0009, ADR 0011).
///
/// # Errors
/// Returns an error if a connection cannot be acquired or the role switch
/// fails.
pub async fn app_tx(pool: &PgPool) -> sqlx::Result<Transaction<'static, Postgres>> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET LOCAL ROLE gpq_app")
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// Opens a transaction under `gpq_app` with `gpq.tenant_id` set, exactly as
/// `Db::begin_tenant` does in the real binary (ADR 0011): row-level security
/// then scopes every statement to `tenant`.
///
/// # Errors
/// Returns an error if a connection cannot be acquired or the role/GUC setup
/// fails.
pub async fn tenant_tx(
    pool: &PgPool,
    tenant: Uuid,
) -> sqlx::Result<Transaction<'static, Postgres>> {
    let mut tx = app_tx(pool).await?;
    sqlx::query("SELECT set_config('gpq.tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

/// A deterministic 64-character hex content hash for a given seed, valid
/// against every `~ '^[0-9a-f]{64}$'` check constraint in the schema.
#[must_use]
pub fn content_hash(seed: &str) -> String {
    hex::encode(Sha256::digest(seed.as_bytes()))
}

/// HMAC-SHA256 digest of `secret` under `key`, matching `KeyedHasher::hash`
/// in `src/auth.rs` byte-for-byte (ADR 0009).
#[must_use]
pub fn hmac_digest(key: &[u8; 32], secret: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap_or_else(|err| {
        panic!("HMAC-SHA256 accepts keys of any length: {err}");
    });
    mac.update(secret.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Inserts a `tenants` row, relying on schema defaults for its policy columns.
pub async fn insert_tenant(pool: &PgPool, id: Uuid, name: &str) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(id)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

/// Soft-deletes a Tenant (`deleted_at`), as local administration's `tenant
/// delete` command does.
pub async fn soft_delete_tenant(pool: &PgPool, id: Uuid) -> anyhow::Result<()> {
    sqlx::query("UPDATE tenants SET deleted_at = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Inserts a Tenant Master Key row (ADR 0009: stored only as a keyed hash).
pub async fn insert_master_key(
    pool: &PgPool,
    tenant_id: Uuid,
    id: Uuid,
    key_hash: &[u8],
    expires_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO tenant_master_keys (tenant_id, id, key_hash, expires_at, revoked_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(tenant_id)
    .bind(id)
    .bind(key_hash)
    .bind(expires_at)
    .bind(revoked_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Inserts a Worker row with its Worker Credential digest (ADR 0009).
pub async fn insert_worker(
    pool: &PgPool,
    tenant_id: Uuid,
    id: Uuid,
    name: &str,
    credential_hash: &[u8],
    revoked_at: Option<DateTime<Utc>>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO workers (tenant_id, id, name, credential_hash, revoked_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(tenant_id)
    .bind(id)
    .bind(name)
    .bind(credential_hash)
    .bind(revoked_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Inserts a Device Pool with `total_slots`/`free_slots` set explicitly, so
/// leasing and slot-release tests can drive them directly. `free_slots` is a
/// generated column (`total_slots - claimed_slots`, migration `0004`), so
/// this seeds the equivalent `claimed_slots` counter instead.
pub async fn insert_device_pool(
    pool: &PgPool,
    tenant_id: Uuid,
    id: Uuid,
    worker_id: Uuid,
    pool_key: &str,
    total_slots: i32,
    free_slots: i32,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO device_pools \
            (tenant_id, id, worker_id, pool_key, backend_kind, total_slots, claimed_slots) \
         VALUES ($1, $2, $3, $4, 'llama_cpp', $5, $6)",
    )
    .bind(tenant_id)
    .bind(id)
    .bind(worker_id)
    .bind(pool_key)
    .bind(total_slots)
    .bind(total_slots - free_slots)
    .execute(pool)
    .await?;
    Ok(())
}

/// One Generation fixture, with defaults for every `NOT NULL` column that a
/// given test does not care about.
pub struct GenerationSpec {
    pub tenant_id: Uuid,
    pub id: Uuid,
    pub state: &'static str,
    pub target_kind: &'static str,
    pub version_sha256: String,
    pub priority: i16,
    pub attempt_count: i32,
    pub accepted_attempt_id: Option<Uuid>,
    pub failure_kind: Option<&'static str>,
}

impl GenerationSpec {
    #[must_use]
    pub fn new(tenant_id: Uuid, version_sha256: String) -> Self {
        Self {
            tenant_id,
            id: Uuid::now_v7(),
            state: "queued",
            target_kind: "model",
            version_sha256,
            priority: 5,
            attempt_count: 0,
            accepted_attempt_id: None,
            failure_kind: None,
        }
    }
}

/// Inserts a Generation from `spec`, using schema defaults (`modality`,
/// `caller_kind`, `execution_timeout`, `output_placement`, ...) for every
/// column the fixture builder above does not override.
pub async fn insert_generation(pool: &PgPool, spec: &GenerationSpec) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO generations \
            (tenant_id, id, state, modality, caller_kind, target_kind, alias, version_sha256, \
             priority, execution_timeout, output_placement, attempt_count, accepted_attempt_id, \
             failure_kind, failure_message, created_at, updated_at) \
         VALUES \
            ($1, $2, $3, 'llm', 'durable', $4, 'test-alias', $5, \
             $6, interval '30 minutes', 'inline_relay', $7, $8, \
             $9, '', now(), now())",
    )
    .bind(spec.tenant_id)
    .bind(spec.id)
    .bind(spec.state)
    .bind(spec.target_kind)
    .bind(&spec.version_sha256)
    .bind(spec.priority)
    .bind(spec.attempt_count)
    .bind(spec.accepted_attempt_id)
    .bind(spec.failure_kind)
    .execute(pool)
    .await?;
    Ok(spec.id)
}

/// One Attempt fixture, with defaults for the columns most tests do not vary.
pub struct AttemptSpec {
    pub tenant_id: Uuid,
    pub id: Uuid,
    pub generation_id: Uuid,
    pub attempt_number: i32,
    pub state: &'static str,
    pub worker_id: Uuid,
    pub pool_id: Uuid,
    pub lease_expires_at: DateTime<Utc>,
}

impl AttemptSpec {
    #[must_use]
    pub fn new(
        tenant_id: Uuid,
        generation_id: Uuid,
        worker_id: Uuid,
        pool_id: Uuid,
        lease_expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            tenant_id,
            id: Uuid::now_v7(),
            generation_id,
            attempt_number: 1,
            state: "leased",
            worker_id,
            pool_id,
            lease_expires_at,
        }
    }
}

/// Inserts an Attempt from `spec`.
pub async fn insert_attempt(pool: &PgPool, spec: &AttemptSpec) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO attempts \
            (tenant_id, id, generation_id, attempt_number, state, worker_id, pool_id, slot_key, \
             lease_expires_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'slot-0', $8)",
    )
    .bind(spec.tenant_id)
    .bind(spec.id)
    .bind(spec.generation_id)
    .bind(spec.attempt_number)
    .bind(spec.state)
    .bind(spec.worker_id)
    .bind(spec.pool_id)
    .bind(spec.lease_expires_at)
    .execute(pool)
    .await?;
    Ok(spec.id)
}

/// Inserts an output Artifact exactly as `db::artifacts::record_output` does
/// (ADR 0008), including the one-hour expiry from completion.
pub async fn insert_output_artifact(
    conn: &mut PgConnection,
    tenant_id: Uuid,
    generation_id: Uuid,
    attempt_id: Uuid,
    worker_id: Uuid,
) -> anyhow::Result<Uuid> {
    let id = Uuid::now_v7();
    let ttl_seconds = gpq_domain::OUTPUT_ARTIFACT_TTL.as_secs_f64();
    sqlx::query(
        "INSERT INTO artifacts \
            (tenant_id, id, generation_id, attempt_id, direction, state, placement, \
             size_bytes, digest_sha256, kind, mime_type, worker_id, available_at, expires_at) \
         VALUES \
            ($1, $2, $3, $4, 'output', 'available', 'worker_local', \
             1024, $5, 'text', 'text/plain', $6, now(), now() + make_interval(secs => $7))",
    )
    .bind(tenant_id)
    .bind(id)
    .bind(generation_id)
    .bind(attempt_id)
    .bind(content_hash("artifact"))
    .bind(worker_id)
    .bind(ttl_seconds)
    .execute(conn)
    .await?;
    Ok(id)
}

/// Inserts a `pending` input Artifact not yet linked to any Generation,
/// mirroring `db::artifacts::create_input`.
pub async fn insert_input_artifact(pool: &PgPool, tenant_id: Uuid) -> anyhow::Result<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO artifacts (tenant_id, id, direction, state, placement, size_bytes, digest_sha256, kind, mime_type) \
         VALUES ($1, $2, 'input', 'available', 'inline_relay', 1024, $3, 'text', 'text/plain')",
    )
    .bind(tenant_id)
    .bind(id)
    .bind(content_hash("input"))
    .execute(pool)
    .await?;
    Ok(id)
}

/// Links an existing input Artifact to a Generation, mirroring
/// `admission::link_input_artifacts`.
pub async fn link_input_artifact(
    pool: &PgPool,
    tenant_id: Uuid,
    artifact_id: Uuid,
    generation_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query("UPDATE artifacts SET generation_id = $1 WHERE tenant_id = $2 AND id = $3")
        .bind(generation_id)
        .bind(tenant_id)
        .bind(artifact_id)
        .execute(pool)
        .await?;
    Ok(())
}
