//! Database access root: connection pool, tenant-scoped transactions, and
//! schema-version enforcement.
//!
//! ADR 0011: tenant-owned rows share one schema with forced row-level
//! security; every tenant-scoped transaction sets `gpq.tenant_id` via
//! `set_config(..., true)` so `gpq_current_tenant()` (see the migration)
//! filters every query, even one the application forgot to filter itself.
//! ADR 0013: `PostgreSQL` rows are the only queue truth; `now()` below is the
//! one clock the queue trusts. ADR 0016: `serve` refuses to start against an
//! unexpected migration version.

pub mod artifacts;
pub mod attempts;
pub mod catalog;
pub mod events;
pub mod generations;
pub mod tenants;
pub mod workers;

use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use gpq_domain::{TenantId, WorkerId};
use sqlx::migrate::Migrate;
use sqlx::postgres::PgPoolOptions;
use sqlx::postgres::types::PgInterval;
use sqlx::{PgPool, Postgres, Transaction};

use crate::auth::KeyedHasher;
use crate::config::RemoteConfig;

/// Embedded migrations from `crates/gpq-remote/migrations`, run by
/// `gpq-remote migrate` and consulted by [`Db::assert_schema_current`].
pub const MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// Track of applied migrations, matching the table `sqlx::migrate!()` creates.
const MIGRATIONS_TABLE: &str = "_sqlx_migrations";

/// Maximum pooled connections held open to `PostgreSQL` by one Remote instance.
const MAX_POOL_CONNECTIONS: u32 = 16;

/// Converts a `PostgreSQL` `interval` read back from a column this crate
/// only ever writes as a whole [`Duration`] in `microseconds` (`months` and
/// `days` are always zero in practice); both are still folded in
/// defensively, using checked arithmetic so a corrupt or hand-edited row
/// cannot silently overflow into a nonsensical [`Duration`]. Shared by
/// `catalog` and `tenants`, which used to each carry a slightly different
/// copy of this decode.
pub(crate) fn interval_to_duration(
    context: &str,
    interval: PgInterval,
) -> Result<Duration, sqlx::Error> {
    let PgInterval {
        months,
        days,
        microseconds,
    } = interval;
    let day_micros = i64::from(days)
        .checked_mul(86_400_000_000)
        .ok_or_else(|| sqlx::Error::decode(format!("{context}: interval day overflow")))?;
    let month_micros = i64::from(months)
        .checked_mul(30 * 86_400_000_000)
        .ok_or_else(|| sqlx::Error::decode(format!("{context}: interval month overflow")))?;
    let total_micros = microseconds
        .checked_add(day_micros)
        .and_then(|v| v.checked_add(month_micros))
        .ok_or_else(|| sqlx::Error::decode(format!("{context}: interval overflow")))?;
    let micros = u64::try_from(total_micros)
        .map_err(|err| sqlx::Error::decode(format!("{context}: negative interval: {err}")))?;
    Ok(Duration::from_micros(micros))
}

/// The application's `PostgreSQL` connection pool plus the keyed hasher used to
/// authenticate Master Keys and Worker Credentials.
#[derive(Clone)]
pub struct Db {
    pool: PgPool,
    hasher: KeyedHasher,
}

impl Db {
    /// Connects a pool sized for one Remote instance (ADR 0010) using the
    /// forced-RLS application credential.
    ///
    /// # Errors
    /// Returns an error if the connection string is invalid or `PostgreSQL` is
    /// unreachable.
    pub async fn connect(config: &RemoteConfig) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(MAX_POOL_CONNECTIONS)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET application_name = 'gpq-remote'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&config.database_url)
            .await
            .context("failed to connect to PostgreSQL")?;
        Ok(Self {
            pool,
            hasher: KeyedHasher::new(config.credential_key),
        })
    }

    /// The underlying connection pool, for callers that need raw access
    /// (e.g. `PgListener::connect_with`, ADR 0013).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The keyed hasher used for Master Key and Worker Credential digests.
    #[must_use]
    pub fn hasher(&self) -> &KeyedHasher {
        &self.hasher
    }

    /// Opens a transaction with no tenant GUC set.
    ///
    /// Only for administration (Tenant lifecycle, credential authentication
    /// before the Tenant is known) and migration bookkeeping — every other
    /// query must go through [`Db::begin_tenant`] so RLS scopes it.
    ///
    /// # Errors
    /// Returns an error if a connection cannot be acquired.
    pub async fn begin(&self) -> sqlx::Result<Transaction<'static, Postgres>> {
        self.pool.begin().await
    }

    /// Opens a transaction scoped to `tenant` by setting the `gpq.tenant_id`
    /// GUC that every row-level security policy checks (ADR 0011). The
    /// setting is transaction-local (`set_config(..., true)`) and reverts
    /// automatically at commit or rollback.
    ///
    /// # Errors
    /// Returns an error if a connection cannot be acquired or the GUC cannot
    /// be set.
    pub async fn begin_tenant(
        &self,
        tenant: TenantId,
    ) -> sqlx::Result<Transaction<'static, Postgres>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('gpq.tenant_id', $1, true)")
            .bind(tenant.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }

    /// Refuses to serve against an unexpected migration version (ADR 0016):
    /// a half-applied (dirty) migration or a version other than the one this
    /// binary was built against both fail loudly instead of serving traffic
    /// against a schema the code does not understand.
    ///
    /// # Errors
    /// Returns an error if the schema is dirty or does not match the version
    /// embedded in this binary.
    pub async fn assert_schema_current(&self) -> anyhow::Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .context("failed to acquire a connection to check the schema version")?;
        if let Some(dirty_version) = conn
            .dirty_version(MIGRATIONS_TABLE)
            .await
            .context("failed to read migration state")?
        {
            bail!(
                "migration {dirty_version} is dirty (a prior `gpq-remote migrate` run did not \
                 complete); repair it before serving"
            );
        }
        let applied = conn
            .list_applied_migrations(MIGRATIONS_TABLE)
            .await
            .context("failed to list applied migrations")?;
        let applied_max = applied.iter().map(|migration| migration.version).max();
        let expected_max = MIGRATOR.iter().map(|migration| migration.version).max();
        if applied_max != expected_max {
            bail!(
                "database schema is at version {applied_max:?} but this binary expects \
                 {expected_max:?}; run `gpq-remote migrate` first"
            );
        }
        Ok(())
    }

    /// Authenticates a presented Tenant Master Key, returning the owning
    /// Tenant if it is live (not revoked, not expired).
    ///
    /// The Tenant is unknown before this lookup succeeds, so the query cannot
    /// run under the tenant-scoped policies that ADR 0011 forces on
    /// `tenant_master_keys`. Migration `0002_credential_lookup` exposes
    /// `gpq_authenticate_master_key`, a `SECURITY DEFINER` function owned by the
    /// administration role that returns the Tenant identifier and nothing else
    /// (ADR 0009 stores only keyed hashes, never the secret).
    ///
    /// # Errors
    /// Returns an error if the database query fails.
    pub async fn authenticate_master_key(
        &self,
        presented: &str,
    ) -> anyhow::Result<Option<TenantId>> {
        let digest = self.hasher.hash(presented);
        let tenant_id: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT gpq_authenticate_master_key($1)")
                .bind(&digest)
                .fetch_one(&self.pool)
                .await
                .context("failed to look up the Tenant Master Key")?;
        Ok(tenant_id.map(TenantId::from_uuid))
    }

    /// Authenticates a presented Worker Credential, returning the owning
    /// Tenant and Worker if it is live.
    ///
    /// Uses the same definer-rights path as [`Db::authenticate_master_key`],
    /// for the same reason: a Worker presents a bare credential and its Tenant
    /// is exactly what the lookup discovers.
    ///
    /// # Errors
    /// Returns an error if the database query fails.
    pub async fn authenticate_worker(
        &self,
        presented: &str,
    ) -> anyhow::Result<Option<(TenantId, WorkerId)>> {
        let digest = self.hasher.hash(presented);
        let row: Option<(uuid::Uuid, uuid::Uuid)> =
            sqlx::query_as("SELECT tenant_id, worker_id FROM gpq_authenticate_worker($1)")
                .bind(&digest)
                .fetch_optional(&self.pool)
                .await
                .context("failed to look up the Worker Credential")?;
        Ok(row.map(|(tenant_id, worker_id)| {
            (
                TenantId::from_uuid(tenant_id),
                WorkerId::from_uuid(worker_id),
            )
        }))
    }

    /// The database's own clock — the only one the queue trusts for lease
    /// and timeout math (ADR 0013).
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub async fn now(&self) -> sqlx::Result<DateTime<Utc>> {
        let (now,): (DateTime<Utc>,) = sqlx::query_as("SELECT now()").fetch_one(&self.pool).await?;
        Ok(now)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::postgres::types::PgInterval;

    use super::interval_to_duration;

    #[test]
    fn interval_round_trips_seconds() {
        let duration = Duration::from_secs(3_723);
        let Ok(interval) = PgInterval::try_from(duration) else {
            panic!("encode");
        };
        let Ok(back) = interval_to_duration("test", interval) else {
            panic!("decode");
        };
        assert_eq!(back, duration);
    }

    #[test]
    fn interval_months_fold_into_thirty_day_periods() {
        let interval = PgInterval {
            months: 1,
            days: 0,
            microseconds: 0,
        };
        let Ok(duration) = interval_to_duration("test", interval) else {
            panic!("decode");
        };
        assert_eq!(duration, Duration::from_hours(30 * 24));
    }

    #[test]
    fn interval_days_are_folded_into_a_fixed_day() {
        let interval = PgInterval {
            months: 0,
            days: 2,
            microseconds: 0,
        };
        let Ok(duration) = interval_to_duration("test", interval) else {
            panic!("decode");
        };
        assert_eq!(duration, Duration::from_hours(48));
    }

    #[test]
    fn interval_days_and_microseconds_combine() {
        let interval = PgInterval {
            months: 0,
            days: 1,
            microseconds: 3_600_000_000,
        };
        let Ok(duration) = interval_to_duration("test", interval) else {
            panic!("decode");
        };
        assert_eq!(duration, Duration::from_hours(25));
    }
}
