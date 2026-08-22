//! Live Worker session registry.
//!
//! ADR 0010: Remote holds no durable state of its own; the only in-memory
//! state it keeps is the set of currently connected Worker control sessions,
//! used to route outbound [`RemoteMessage`]s and to answer "is this Worker
//! online" questions for scheduling and `OpenAI` model availability (ADR 0006).
//! Losing this map is safe: a reconnecting Worker re-advertises its
//! capabilities and Remote's database state is otherwise authoritative.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use gpq_domain::{TenantId, WorkerId};
use gpq_proto::gpq::worker::v1::RemoteMessage;
use tokio::sync::mpsc;

/// One live Worker control session.
struct SessionEntry {
    tenant_id: TenantId,
    session_id: String,
    outbound: mpsc::Sender<RemoteMessage>,
}

/// The set of Workers currently holding an open
/// `WorkerSessionService::Session` stream.
///
/// Cloning shares the same underlying map (ADR 0010): every clone observes
/// registrations and drops made through any other clone.
#[derive(Clone, Default)]
pub struct WorkerRegistry {
    sessions: Arc<Mutex<HashMap<WorkerId, SessionEntry>>>,
}

/// Outcome of trying to push one `RemoteMessage` into a Worker's outbound
/// control channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    /// Queued on the Worker's live outbound channel.
    Delivered,
    /// The Worker has a live session but its outbound channel is full; the
    /// Worker is NOT gone and the caller must not treat this as offline.
    Backpressured,
    /// No live session, or the outbound channel is closed (the session is
    /// tearing down).
    Offline,
}

impl SendOutcome {
    /// Whether the message was actually queued for delivery.
    #[must_use]
    pub fn is_delivered(self) -> bool {
        matches!(self, Self::Delivered)
    }
}

impl WorkerRegistry {
    /// Registers a live session for `worker`, replacing any prior session for
    /// the same Worker.
    ///
    /// Returns a [`SessionGuard`] that removes the registration on drop, but
    /// only if it is still the current session: a guard for a session that
    /// was superseded by a later `register` call (e.g. after the Worker
    /// reconnected before the old stream noticed) is a no-op on drop, so a
    /// late-arriving cleanup never evicts a newer, live session.
    #[must_use]
    pub fn register(
        &self,
        tenant: TenantId,
        worker: WorkerId,
        session_id: String,
        outbound: mpsc::Sender<RemoteMessage>,
    ) -> SessionGuard {
        let entry = SessionEntry {
            tenant_id: tenant,
            session_id: session_id.clone(),
            outbound,
        };
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.insert(worker, entry);
        }
        SessionGuard {
            registry: self.clone(),
            worker_id: worker,
            session_id,
        }
    }

    /// Sends `message` to `worker`'s live session, if any.
    ///
    /// Returns [`SendOutcome::Offline`] when the Worker has no live session
    /// or its outbound channel is closed (the session is tearing down); the
    /// caller should treat this the same as the Worker being offline.
    /// Returns [`SendOutcome::Backpressured`] when the Worker has a live
    /// session but its outbound channel is momentarily full — this is
    /// distinct from offline and must not be treated as though the Worker
    /// is gone.
    #[must_use]
    pub fn send(&self, worker: WorkerId, message: RemoteMessage) -> SendOutcome {
        let outbound = match self.sessions.lock() {
            Ok(sessions) => sessions.get(&worker).map(|entry| entry.outbound.clone()),
            Err(_) => None,
        };
        match outbound {
            Some(outbound) => match outbound.try_send(message) {
                Ok(()) => SendOutcome::Delivered,
                Err(mpsc::error::TrySendError::Full(_)) => SendOutcome::Backpressured,
                Err(mpsc::error::TrySendError::Closed(_)) => SendOutcome::Offline,
            },
            None => SendOutcome::Offline,
        }
    }

    /// Whether `worker` currently holds a live control session.
    #[must_use]
    pub fn is_online(&self, worker: WorkerId) -> bool {
        self.sessions
            .lock()
            .is_ok_and(|sessions| sessions.contains_key(&worker))
    }

    /// Every Worker of `tenant` currently holding a live control session.
    #[must_use]
    pub fn online_workers(&self, tenant: TenantId) -> Vec<WorkerId> {
        let Ok(sessions) = self.sessions.lock() else {
            return Vec::new();
        };
        sessions
            .iter()
            .filter(|(_, entry)| entry.tenant_id == tenant)
            .map(|(worker_id, _)| *worker_id)
            .collect()
    }
}

/// Removes a Worker's session registration when the session ends.
///
/// Holding this alive keeps the registration current; dropping it clears the
/// registration unless a newer session already replaced it.
pub struct SessionGuard {
    registry: WorkerRegistry,
    worker_id: WorkerId,
    session_id: String,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        let Ok(mut sessions) = self.registry.sessions.lock() else {
            return;
        };
        let still_current = sessions
            .get(&self.worker_id)
            .is_some_and(|entry| entry.session_id == self.session_id);
        if still_current {
            sessions.remove(&self.worker_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_message() -> RemoteMessage {
        RemoteMessage::default()
    }

    #[test]
    fn registered_worker_is_online_and_reachable() {
        let registry = WorkerRegistry::default();
        let tenant = TenantId::new();
        let worker = WorkerId::new();
        let (tx, mut rx) = mpsc::channel(1);

        let guard = registry.register(tenant, worker, "session-a".to_owned(), tx);

        assert!(registry.is_online(worker));
        assert_eq!(registry.online_workers(tenant), vec![worker]);
        assert_eq!(
            registry.send(worker, worker_message()),
            SendOutcome::Delivered
        );
        assert!(rx.try_recv().is_ok());

        drop(guard);
        assert!(!registry.is_online(worker));
        assert_eq!(
            registry.send(worker, worker_message()),
            SendOutcome::Offline
        );
    }

    #[test]
    fn unknown_worker_is_offline() {
        let registry = WorkerRegistry::default();
        let worker = WorkerId::new();
        assert!(!registry.is_online(worker));
        assert_eq!(
            registry.send(worker, worker_message()),
            SendOutcome::Offline
        );
    }

    #[test]
    fn stale_guard_does_not_evict_a_newer_session() {
        let registry = WorkerRegistry::default();
        let tenant = TenantId::new();
        let worker = WorkerId::new();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);

        let guard_a = registry.register(tenant, worker, "session-a".to_owned(), tx_a);
        let _guard_b = registry.register(tenant, worker, "session-b".to_owned(), tx_b);

        // The old guard's session was superseded; dropping it must not evict
        // the newer registration.
        drop(guard_a);

        assert!(registry.is_online(worker));
        assert_eq!(
            registry.send(worker, worker_message()),
            SendOutcome::Delivered
        );
        assert!(rx_b.try_recv().is_ok());
    }

    #[test]
    fn online_workers_is_scoped_to_its_tenant() {
        let registry = WorkerRegistry::default();
        let tenant_a = TenantId::new();
        let tenant_b = TenantId::new();
        let worker_a = WorkerId::new();
        let worker_b = WorkerId::new();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, _rx_b) = mpsc::channel(1);

        let _guard_a = registry.register(tenant_a, worker_a, "a".to_owned(), tx_a);
        let _guard_b = registry.register(tenant_b, worker_b, "b".to_owned(), tx_b);

        assert_eq!(registry.online_workers(tenant_a), vec![worker_a]);
        assert_eq!(registry.online_workers(tenant_b), vec![worker_b]);
    }

    #[test]
    fn send_reports_backpressure_without_marking_the_worker_offline() {
        let registry = WorkerRegistry::default();
        let tenant = TenantId::new();
        let worker = WorkerId::new();
        let (tx, _rx) = mpsc::channel(1);

        let _guard = registry.register(tenant, worker, "session-a".to_owned(), tx);

        // First send fills the capacity-1 channel; nothing has drained it.
        assert_eq!(
            registry.send(worker, worker_message()),
            SendOutcome::Delivered
        );
        // The channel is full but the session is still live: this must not
        // be conflated with the Worker being offline.
        assert_eq!(
            registry.send(worker, worker_message()),
            SendOutcome::Backpressured
        );
        assert!(registry.is_online(worker));
    }
}
