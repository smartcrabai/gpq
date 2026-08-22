//! Attempt leases.
//!
//! ADR 0003: Workers heartbeat every 10 seconds against a 45-second lease, may
//! resume the same Attempt after reconnecting before expiry, and must
//! cooperatively cancel after expiry; results committed under an expired lease
//! are rejected. ADR 0013 makes database time the only clock that matters, so
//! every predicate here takes an explicit `now`.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// Heartbeat cadence expected from Workers.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Lifetime of a lease, refreshed by every accepted heartbeat.
pub const LEASE_TTL: Duration = Duration::from_secs(45);

/// Absolute expiry for a lease created or renewed at `now`.
#[must_use]
pub fn lease_expiry_from(now: DateTime<Utc>) -> DateTime<Utc> {
    now + chrono::Duration::from_std(LEASE_TTL).unwrap_or(chrono::Duration::seconds(45))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_expires_forty_five_seconds_after_now() {
        let now = Utc::now();
        let expiry = lease_expiry_from(now);
        assert_eq!(expiry, now + chrono::Duration::seconds(45));
    }
}
