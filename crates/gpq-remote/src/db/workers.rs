//! Worker, Device Pool, and pool-Model persistence.
//!
//! ADR 0001/0011: every query below is scoped to a `tenant_id` and is meant
//! to run inside a tenant-scoped transaction (`Db::begin_tenant`); RLS refuses
//! rows for any other Tenant regardless. ADR 0005 models one row per Device
//! Pool (`device_pools`) with a `free_slots` counter rather than a per-Slot
//! table — the domain's [`gpq_domain::SlotCapability`] is therefore
//! synthesized one-per-Pool, not one-per-Slot. Runtime OOM never persists
//! capability here; retry exclusion is scoped to the Generation (ADR 0003). ADR 0009 stores only
//! keyed hashes of Worker Credentials. ADR 0012 pins every pool-Model row to
//! an immutable Model Version registered by [`crate::db::catalog`] under the
//! canonical hash of its `ModelManifest`.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use gpq_domain::hash::ContentHashError;
use gpq_domain::{
    ContentHash, DevicePoolId, ModelManifest, ShapeLimits, SlotCapability, TenantId, WorkerId,
};
use sqlx::types::Json;
use uuid::Uuid;

/// Wraps a parse failure from a stored column as a `sqlx` decode error.
fn decode_hash(hex: &str) -> Result<ContentHash, sqlx::Error> {
    hex.parse::<ContentHash>()
        .map_err(|err: ContentHashError| sqlx::Error::Decode(Box::new(err)))
}

/// A Worker's self-reported enrollment identity (ADR 0009), grouped so
/// [`enroll`] stays under clippy's argument-count ceiling.
pub struct WorkerEnrollment<'a> {
    /// Worker-chosen display name, unique per Tenant.
    pub name: &'a str,
    /// Stable host identity: OS, architecture, and hostname.
    pub host_descriptor: &'a str,
    /// Reported `gpq-worker` version string.
    pub worker_version: &'a str,
    /// Negotiated protocol major version.
    pub protocol_major: u32,
    /// Negotiated protocol minor version.
    pub protocol_minor: u32,
    /// Keyed hash of the freshly generated Worker Credential (ADR 0009).
    pub credential_hash: &'a [u8],
}

/// Registers a Worker for `tenant_id`, or re-enrolls an existing one.
///
/// Idempotent by `(tenant_id, name)`: a second enrollment of the same name
/// rotates the stored credential hash, refreshes the reported host/version,
/// and clears `revoked_at`, but keeps the Worker's identity (ADR 0009).
///
/// # Errors
/// Returns [`sqlx::Error`] if the insert/upsert fails, e.g. the connection
/// is lost; the `ON CONFLICT (tenant_id, name) DO UPDATE` means a second
/// enrollment of the same name never surfaces as a unique-constraint
/// violation.
pub async fn enroll(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    enrollment: WorkerEnrollment<'_>,
) -> sqlx::Result<WorkerId> {
    let protocol_major = i32::try_from(enrollment.protocol_major).unwrap_or(i32::MAX);
    let protocol_minor = i32::try_from(enrollment.protocol_minor).unwrap_or(i32::MAX);
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO workers \
            (tenant_id, id, name, host_descriptor, worker_version, protocol_major, protocol_minor, credential_hash) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
         ON CONFLICT (tenant_id, name) DO UPDATE SET \
            host_descriptor = excluded.host_descriptor, \
            worker_version = excluded.worker_version, \
            protocol_major = excluded.protocol_major, \
            protocol_minor = excluded.protocol_minor, \
            credential_hash = excluded.credential_hash, \
            revoked_at = NULL, \
            enrolled_at = now() \
         RETURNING id",
    )
    .bind(tenant_id.as_uuid())
    .bind(WorkerId::new().as_uuid())
    .bind(enrollment.name)
    .bind(enrollment.host_descriptor)
    .bind(enrollment.worker_version)
    .bind(protocol_major)
    .bind(protocol_minor)
    .bind(enrollment.credential_hash)
    .fetch_one(&mut *conn)
    .await?;
    Ok(WorkerId::from_uuid(id))
}

/// Revokes `worker`'s credential. Idempotent: revoking an already-revoked
/// Worker is a no-op.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection lost).
/// Revoking a Worker that does not exist, or is already revoked, is not
/// an error: the `WHERE` clause simply matches zero rows.
pub async fn revoke_worker(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE workers SET revoked_at = now() \
         WHERE tenant_id = $1 AND id = $2 AND revoked_at IS NULL",
    )
    .bind(tenant_id.as_uuid())
    .bind(worker_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Cheap re-check that `worker_id` is still enrolled and unrevoked (ADR
/// 0009), for the heartbeat path (`session::try_handle_heartbeat`).
///
/// `Db::authenticate_worker` only runs once, at Session establishment;
/// nothing else re-checks a credential revoked mid-Session, so a revoked
/// Worker would otherwise keep leasing Generations and receiving Tenant
/// prompt data for the rest of its stream's life. Deliberately a single
/// scalar query rather than re-running full credential authentication: the
/// credential secret is not in hand at heartbeat time, only the
/// already-authenticated `worker_id`.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails. A `worker_id` that does not
/// exist is not an error: it returns `Ok(false)`, same as a revoked Worker.
pub async fn is_enrolled_and_unrevoked(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
) -> sqlx::Result<bool> {
    let live: Option<bool> = sqlx::query_scalar(
        "SELECT revoked_at IS NULL FROM workers WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(worker_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(live.unwrap_or(false))
}

/// A summary row of one Worker, for administration and catalog listings.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct WorkerSummary {
    /// Row-mapped identifier; wrap with [`WorkerId::from_uuid`] at call sites
    /// that need the typed id (kept raw here so `sqlx::FromRow` applies).
    #[sqlx(rename = "id")]
    pub id_uuid: Uuid,
    /// Worker-chosen display name, unique per Tenant.
    pub name: String,
    /// Last reported `gpq-worker` version string.
    pub worker_version: String,
    /// Last time this Worker sent a heartbeat or capability report.
    pub last_seen_at: Option<DateTime<Utc>>,
    /// When the Worker's credential was revoked, if ever.
    pub revoked_at: Option<DateTime<Utc>>,
}

impl WorkerSummary {
    /// The typed identifier of this Worker.
    #[must_use]
    pub const fn id(&self) -> WorkerId {
        WorkerId::from_uuid(self.id_uuid)
    }
}

/// Every Worker enrolled for `tenant_id`, most recently enrolled first.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or a row cannot be decoded
/// into [`WorkerSummary`].
pub async fn list_workers(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<WorkerSummary>> {
    sqlx::query_as(
        "SELECT id, name, worker_version, last_seen_at, revoked_at \
         FROM workers WHERE tenant_id = $1 ORDER BY enrolled_at DESC",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await
}

/// Raw row shape of the `list_pools` query, before domain mapping.
#[derive(Debug, sqlx::FromRow)]
struct PoolRowRaw {
    worker_id: Uuid,
    pool_id: Uuid,
    accelerator: String,
    accelerator_version: String,
    total_slots: i32,
    free_slots: i32,
    resident_model_sha256: Option<String>,
    accelerator_memory_bytes: Option<i64>,
    shape_limits: Json<ShapeLimits>,
    image_lora_sha256: Vec<String>,
    model_hashes: Vec<String>,
}

/// One Device Pool and its advertised Model Versions, for catalog listings.
#[derive(Debug, Clone)]
pub struct PoolRow {
    /// The owning Worker.
    pub worker_id: WorkerId,
    /// Surrogate identifier assigned by Remote.
    pub pool_id: DevicePoolId,
    /// Accelerator the Pool runs on, as advertised by its Worker.
    pub accelerator: String,
    /// Observed accelerator or runtime version string.
    pub accelerator_version: String,
    /// Total concurrent Execution Slots.
    pub total_slots: u32,
    /// Currently free Execution Slots.
    pub free_slots: u32,
    /// Model Version currently loaded, if any.
    pub resident_model_sha256: Option<ContentHash>,
    /// Accelerator memory reported by the Worker, when known.
    pub accelerator_memory_bytes: Option<u64>,
    /// Shape limits the Pool advertises for native generation.
    pub shape_limits: ShapeLimits,
    /// Content hashes of every image `LoRA` installed for this Pool.
    pub image_loras: Vec<ContentHash>,
    /// Content hashes of every Model Version installed for this Pool.
    pub model_hashes: Vec<ContentHash>,
}

impl TryFrom<PoolRowRaw> for PoolRow {
    type Error = sqlx::Error;

    fn try_from(row: PoolRowRaw) -> Result<Self, Self::Error> {
        let resident_model_sha256 = row
            .resident_model_sha256
            .as_deref()
            .map(decode_hash)
            .transpose()?;
        let image_loras: Vec<ContentHash> = row
            .image_lora_sha256
            .iter()
            .map(|hex| decode_hash(hex))
            .collect::<Result<_, _>>()?;
        let model_hashes: Vec<ContentHash> = row
            .model_hashes
            .iter()
            .map(|hex| decode_hash(hex))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            worker_id: WorkerId::from_uuid(row.worker_id),
            pool_id: DevicePoolId::from_uuid(row.pool_id),
            accelerator: row.accelerator,
            accelerator_version: row.accelerator_version,
            total_slots: u32::try_from(row.total_slots).unwrap_or(0),
            free_slots: u32::try_from(row.free_slots).unwrap_or(0),
            resident_model_sha256,
            accelerator_memory_bytes: row
                .accelerator_memory_bytes
                .and_then(|value| u64::try_from(value).ok()),
            shape_limits: row.shape_limits.0,
            image_loras,
            model_hashes,
        })
    }
}

/// Every Device Pool of `tenant_id`'s Workers, with their installed Model
/// Versions and image `LoRAs`, for Native Catalog listings.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if a row's
/// `resident_model_sha256`, `image_lora_sha256`, or model hash column holds a
/// value that cannot be decoded (see [`decode_hash`]) — in practice only
/// possible if the stored column was corrupted.
pub async fn list_pools(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<PoolRow>> {
    let rows: Vec<PoolRowRaw> = sqlx::query_as(
        "SELECT \
            dp.worker_id AS worker_id, \
            dp.id AS pool_id, \
            dp.accelerator AS accelerator, \
            dp.backend_version AS accelerator_version, \
            dp.total_slots AS total_slots, \
            dp.free_slots AS free_slots, \
            dp.resident_model_sha256 AS resident_model_sha256, \
            dp.accelerator_memory_bytes AS accelerator_memory_bytes, \
            dp.shape_limits AS shape_limits, \
            dp.image_lora_sha256 AS image_lora_sha256, \
            COALESCE(( \
                SELECT array_agg(pm.content_sha256) \
                FROM pool_models pm \
                WHERE pm.tenant_id = dp.tenant_id AND pm.pool_id = dp.id \
            ), ARRAY[]::text[]) AS model_hashes \
         FROM device_pools dp \
         WHERE dp.tenant_id = $1",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter().map(PoolRow::try_from).collect()
}

/// Verifies the Worker's credential and serializes messages that mutate its row.
///
/// Lock before the caller's update: concurrent `FOR SHARE` locks deadlock when
/// both message handlers then upgrade them.
/// Keep this lock until the caller commits so credential revocation cannot race
/// with an authenticated message.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails (e.g. connection lost).
pub async fn lock_current_worker_credential(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
    credential_hash: &[u8],
) -> sqlx::Result<bool> {
    let live: Option<bool> = sqlx::query_scalar(
        "SELECT credential_hash = $3 AND revoked_at IS NULL \
         FROM workers WHERE tenant_id = $1 AND id = $2 FOR NO KEY UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(worker_id.as_uuid())
    .bind(credential_hash)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(live.unwrap_or(false))
}

/// Records the Worker's freshly opened control session.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection lost).
pub async fn mark_session(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
    session_id: &str,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE workers SET session_id = $3, last_seen_at = now() \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(worker_id.as_uuid())
    .bind(session_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Clears the Worker's session marker when its control stream ends.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection lost).
pub async fn clear_session(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
) -> sqlx::Result<()> {
    sqlx::query("UPDATE workers SET session_id = NULL WHERE tenant_id = $1 AND id = $2")
        .bind(tenant_id.as_uuid())
        .bind(worker_id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Refreshes `last_seen_at` for a heartbeat or capability report.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection lost).
pub async fn touch_last_seen(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
) -> sqlx::Result<()> {
    sqlx::query("UPDATE workers SET last_seen_at = now() WHERE tenant_id = $1 AND id = $2")
        .bind(tenant_id.as_uuid())
        .bind(worker_id.as_uuid())
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// One Device Pool as advertised in a Worker's capability report
/// (ADR 0005), ready to persist.
///
/// Callers register every hash in `model_hashes` and `image_loras` with
/// their owning tables before upserting (Model Versions via
/// [`crate::db::catalog::register_model_versions`]).
#[derive(Debug, Clone)]
pub struct PoolUpsert {
    /// Pool identity as configured on the Worker host.
    pub pool_key: String,
    /// Accelerator the Pool runs on, as advertised by its Worker.
    pub accelerator: String,
    /// Observed accelerator or runtime version string.
    pub accelerator_version: String,
    /// Whether the Pool currently accepts work.
    pub ready: bool,
    /// Human-readable reason when `ready` is false.
    pub unready_reason: String,
    /// Total concurrent Execution Slots.
    pub total_slots: u32,
    /// Model Version currently loaded, if any.
    pub resident_model_sha256: Option<ContentHash>,
    /// Accelerator memory reported by the Worker, when known.
    pub accelerator_memory_bytes: Option<u64>,
    /// Probe results per required native operation.
    pub probes: BTreeMap<String, bool>,
    /// Content hashes of every Model Version installed for this Pool.
    pub model_hashes: Vec<ContentHash>,
    /// Content hashes of every image `LoRA` installed for this Pool.
    pub image_loras: Vec<ContentHash>,
    /// Shape limits the Pool advertises for native generation.
    pub shape_limits: ShapeLimits,
}

/// Replaces `worker_id`'s Device Pools and their pool-Model rows with the
/// freshly advertised set.
///
/// Pools no longer advertised are deleted (cascading to their `pool_models`
/// rows); Pools that remain keep their surrogate id. Advertised Models not
/// previously tracked for a Pool are added; Models no longer advertised are
/// removed. Pool-Model rows carry no failure state: a runtime OOM is scoped to
/// the Generation that hit it (ADR 0003), so re-advertising never resurrects or
/// hides capability.
///
/// Deliberately never writes `claimed_slots`/`free_slots`: a Worker's
/// self-reported free-Slot count raced with Remote's own
/// [`claim_slot`]/[`release_slot`] accounting and could reset an
/// outstanding claim the instant a capability report arrived, before the
/// Worker had even received the lease it was claimed for. `claimed_slots`
/// is Remote-owned and `free_slots` is a generated column
/// (`total_slots - claimed_slots`, migration `0004`); only `total_slots`
/// and the genuinely Worker-owned descriptive fields are refreshed here. A
/// Worker that disappears mid-lease leaks its claim only until the
/// lease-expiry sweep (`expiry.rs`) fails the stranded Attempt and calls
/// [`release_slot`].
///
/// Callers MUST invoke this inside an existing transaction (`begin_tenant`)
/// for the Pool-set and Model-set replacement to be atomic.
///
/// # Errors
/// Returns [`sqlx::Error`] if any of the delete/insert/upsert statements
/// fail, e.g. on a lost connection. Callers that do not wrap this in a
/// transaction risk a partially applied Pool set on such a failure.
pub async fn upsert_pools(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
    pools: &[PoolUpsert],
) -> sqlx::Result<()> {
    let pool_keys: Vec<&str> = pools.iter().map(|pool| pool.pool_key.as_str()).collect();
    sqlx::query(
        "DELETE FROM device_pools WHERE tenant_id = $1 AND worker_id = $2 AND NOT (pool_key = ANY($3))",
    )
    .bind(tenant_id.as_uuid())
    .bind(worker_id.as_uuid())
    .bind(&pool_keys)
    .execute(&mut *conn)
    .await?;

    for pool in pools {
        let total_slots = i32::try_from(pool.total_slots).unwrap_or(i32::MAX);
        let resident_model_sha256 = pool.resident_model_sha256.map(|hash| hash.to_hex());
        let accelerator_memory_bytes = pool
            .accelerator_memory_bytes
            .and_then(|bytes| i64::try_from(bytes).ok());
        let image_lora_hex: Vec<String> =
            pool.image_loras.iter().map(ContentHash::to_hex).collect();

        let pool_id: Uuid = sqlx::query_scalar(
            "INSERT INTO device_pools \
                (tenant_id, id, worker_id, pool_key, accelerator, backend_version, ready, \
                 unready_reason, total_slots, resident_model_sha256, \
                 accelerator_memory_bytes, shape_limits, image_lora_sha256, probes, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now()) \
             ON CONFLICT (tenant_id, worker_id, pool_key) DO UPDATE SET \
                accelerator = excluded.accelerator, \
                backend_version = excluded.backend_version, \
                ready = excluded.ready, \
                unready_reason = excluded.unready_reason, \
                total_slots = excluded.total_slots, \
                resident_model_sha256 = excluded.resident_model_sha256, \
                accelerator_memory_bytes = excluded.accelerator_memory_bytes, \
                shape_limits = excluded.shape_limits, \
                image_lora_sha256 = excluded.image_lora_sha256, \
                probes = excluded.probes, \
                updated_at = now() \
             RETURNING id",
        )
        .bind(tenant_id.as_uuid())
        .bind(DevicePoolId::new().as_uuid())
        .bind(worker_id.as_uuid())
        .bind(&pool.pool_key)
        .bind(&pool.accelerator)
        .bind(&pool.accelerator_version)
        .bind(pool.ready)
        .bind(&pool.unready_reason)
        .bind(total_slots)
        .bind(resident_model_sha256)
        .bind(accelerator_memory_bytes)
        .bind(Json(&pool.shape_limits))
        .bind(&image_lora_hex)
        .bind(Json(&pool.probes))
        .fetch_one(&mut *conn)
        .await?;

        let model_hex: Vec<String> = pool.model_hashes.iter().map(ContentHash::to_hex).collect();

        sqlx::query(
            "DELETE FROM pool_models WHERE tenant_id = $1 AND pool_id = $2 AND NOT (content_sha256 = ANY($3))",
        )
        .bind(tenant_id.as_uuid())
        .bind(pool_id)
        .bind(&model_hex)
        .execute(&mut *conn)
        .await?;

        for hash in &model_hex {
            sqlx::query(
                "INSERT INTO pool_models (tenant_id, pool_id, content_sha256) VALUES ($1, $2, $3) \
                 ON CONFLICT (tenant_id, pool_id, content_sha256) DO NOTHING",
            )
            .bind(tenant_id.as_uuid())
            .bind(pool_id)
            .bind(hash)
            .execute(&mut *conn)
            .await?;
        }
    }

    Ok(())
}

/// Atomically claims one free Execution Slot on `pool_id`.
///
/// Slots are tracked by Remote's own `claimed_slots` counter, never by the
/// Worker-reported `free_slots` (which [`upsert_pools`] deliberately never
/// writes), so a capability report racing with a lease can never silently
/// double-book a Slot.
///
/// Returns `true` if a Slot was claimed (the caller may proceed to create a
/// lease), `false` if the Pool had no free Slots (a race with another
/// scheduling pass or a shrinking capability report; the caller must not
/// proceed).
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection lost). A
/// lost race for the last free Slot is not an error: it returns
/// `Ok(false)`.
pub async fn claim_slot(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    pool_id: DevicePoolId,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE device_pools SET claimed_slots = claimed_slots + 1, updated_at = now() \
         WHERE tenant_id = $1 AND id = $2 AND claimed_slots < total_slots",
    )
    .bind(tenant_id.as_uuid())
    .bind(pool_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Releases one Execution Slot back to `pool_id`, floored at zero.
///
/// Called when an Attempt stops occupying a Slot (result committed, failed,
/// cancelled, or its lease expired). A Worker that vanishes mid-lease
/// without this ever running leaks its claim only until the lease-expiry
/// sweep (`expiry.rs`) fails the stranded Attempt, which calls this too.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection lost).
pub async fn release_slot(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    pool_id: DevicePoolId,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE device_pools SET claimed_slots = GREATEST(claimed_slots - 1, 0), updated_at = now() \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(pool_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Raw row shape of the `pool_capabilities` query, before domain mapping.
#[derive(Debug, sqlx::FromRow)]
struct PoolCapabilityRow {
    pool_id: Uuid,
    worker_id: Uuid,
    pool_key: String,
    accelerator: String,
    accelerator_version: String,
    free_slots: i32,
    resident_model_sha256: Option<String>,
    accelerator_memory_bytes: Option<i64>,
    shape_limits: Json<ShapeLimits>,
    image_lora_sha256: Vec<String>,
    /// Manifests of this Pool's capable Model Versions, keyed by hex hash.
    model_manifests: Json<BTreeMap<String, ModelManifest>>,
}

/// Builds the domain [`SlotCapability`] for one ready Pool from its raw row.
///
/// Pure mapping, kept separate from the query so it is unit-testable without
/// a database: no per-Slot table exists (ADR 0005), so exactly one
/// `SlotCapability` is synthesized per ready Pool, paired with its
/// `free_slots` count — see [`gpq_domain::SlotContext`], which already models
/// "one Slot-shaped capability plus a free-count" this way.
fn build_slot_capability(
    tenant_id: TenantId,
    row: PoolCapabilityRow,
) -> Result<(SlotCapability, u32, WorkerId, String), sqlx::Error> {
    let model_manifests = row
        .model_manifests
        .0
        .into_iter()
        .map(
            |(hex, manifest)| -> Result<(ContentHash, ModelManifest), sqlx::Error> {
                Ok((decode_hash(&hex)?, manifest))
            },
        )
        .collect::<Result<BTreeMap<_, _>, sqlx::Error>>()?;
    let image_loras = row
        .image_lora_sha256
        .iter()
        .map(|hex| decode_hash(hex))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let resident_model = row
        .resident_model_sha256
        .as_deref()
        .map(decode_hash)
        .transpose()?;
    let capability = SlotCapability {
        tenant_id,
        worker_id: WorkerId::from_uuid(row.worker_id),
        pool_id: DevicePoolId::from_uuid(row.pool_id),
        slot_id: gpq_domain::SlotId::from_uuid(row.pool_id),
        accelerator: row.accelerator,
        accelerator_version: row.accelerator_version,
        model_manifests,
        image_loras,
        resident_model,
        accelerator_memory_bytes: row
            .accelerator_memory_bytes
            .and_then(|value| u64::try_from(value).ok()),
        shape_limits: row.shape_limits.0,
    };
    let free_slots = u32::try_from(row.free_slots).unwrap_or(0);
    Ok((
        capability,
        free_slots,
        WorkerId::from_uuid(row.worker_id),
        row.pool_key,
    ))
}

/// Every ready Device Pool of `tenant_id`, as a scheduling-ready
/// [`SlotCapability`] paired with its free-Slot count, owning Worker, and
/// Pool key.
///
/// One tuple per ready Pool (ADR 0005: no per-Slot table). Excludes Pools
/// whose Worker's credential has been revoked (ADR 0009): a revoked Worker
/// must never be selected for a fresh lease even before its next heartbeat
/// re-check ends its session (`session::try_handle_heartbeat`). Callers
/// scheduling work should additionally filter to Workers with a live
/// session (`WorkerRegistry::is_online`) — a ready row here only reflects
/// the last capability report, not current connectivity.
///
/// Each capability's `model_manifests` holds every Model Version the Pool
/// advertises. No runtime failure is persisted as capability: retry decisions
/// are scoped to the Generation (see `db::attempts`).
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if a row's content hash,
/// shape-limit, or manifest column cannot be decoded (see
/// [`build_slot_capability`]).
pub async fn pool_capabilities(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<(SlotCapability, u32, WorkerId, String)>> {
    let rows: Vec<PoolCapabilityRow> = sqlx::query_as(
        "SELECT \
            dp.id AS pool_id, \
            dp.worker_id AS worker_id, \
            dp.pool_key AS pool_key, \
            dp.accelerator AS accelerator, \
            dp.backend_version AS accelerator_version, \
            dp.free_slots AS free_slots, \
            dp.resident_model_sha256 AS resident_model_sha256, \
            dp.accelerator_memory_bytes AS accelerator_memory_bytes, \
            dp.shape_limits AS shape_limits, \
            dp.image_lora_sha256 AS image_lora_sha256, \
            COALESCE(( \
                SELECT jsonb_object_agg(mv.content_sha256, mv.manifest) \
                FROM pool_models pm \
                JOIN model_versions mv ON mv.tenant_id = pm.tenant_id AND mv.content_sha256 = pm.content_sha256 \
                WHERE pm.tenant_id = dp.tenant_id AND pm.pool_id = dp.id \
            ), '{}'::jsonb) AS model_manifests \
         FROM device_pools dp \
         JOIN workers w ON w.tenant_id = dp.tenant_id AND w.id = dp.worker_id \
         WHERE dp.tenant_id = $1 AND dp.ready AND w.revoked_at IS NULL",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;

    rows.into_iter()
        .map(|row| build_slot_capability(tenant_id, row))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> ContentHash {
        ContentHash::from_bytes([byte; 32])
    }

    fn empty_shape_limits() -> ShapeLimits {
        let Ok(limits) = serde_json::from_str("{}") else {
            panic!("an empty object is a valid ShapeLimits");
        };
        limits
    }

    fn sample_row() -> PoolCapabilityRow {
        PoolCapabilityRow {
            pool_id: Uuid::nil(),
            worker_id: Uuid::nil(),
            pool_key: "gpu-0".to_owned(),
            accelerator: "cuda".to_owned(),
            accelerator_version: "12.4".to_owned(),
            free_slots: 3,
            resident_model_sha256: Some(hash(1).to_hex()),
            accelerator_memory_bytes: Some(1024),
            shape_limits: Json(empty_shape_limits()),
            image_lora_sha256: vec![hash(3).to_hex()],
            model_manifests: Json(BTreeMap::new()),
        }
    }

    #[test]
    fn builds_slot_capability_from_the_pool_row() {
        let tenant_id = TenantId::new();
        let row = sample_row();

        let Ok((capability, free_slots, worker_id, pool_key)) =
            build_slot_capability(tenant_id, row)
        else {
            panic!("expected a valid capability row");
        };

        assert_eq!(free_slots, 3);
        assert_eq!(pool_key, "gpu-0");
        assert_eq!(worker_id, WorkerId::from_uuid(Uuid::nil()));
        assert_eq!(capability.accelerator, "cuda");
        assert_eq!(capability.accelerator_version, "12.4");
        assert_eq!(capability.resident_model, Some(hash(1)));
        assert_eq!(
            capability.image_loras,
            BTreeSet::from([hash(3)]),
            "image_lora_sha256 feeds image_loras"
        );
    }

    #[test]
    fn free_slots_clamps_negative_database_values_to_zero() {
        let tenant_id = TenantId::new();
        let mut row = sample_row();
        row.free_slots = -1;

        let Ok((_, free_slots, _, _)) = build_slot_capability(tenant_id, row) else {
            panic!("expected a valid capability row");
        };
        assert_eq!(free_slots, 0);
    }

    #[test]
    fn malformed_hash_is_a_decode_error() {
        assert!(decode_hash("not-hex").is_err());
        assert!(decode_hash(&hash(9).to_hex()).is_ok());
    }
}
