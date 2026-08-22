//! The shared application state handed to every transport (Axum, `ConnectRPC`,
//! gRPC) and background task.
//!
//! ADR 0010: Remote is otherwise stateless — every field here is either a
//! handle to durable `PostgreSQL` state ([`crate::db::Db`]), a cheap-to-clone
//! in-process cache that can be rebuilt from Worker reconnection or database
//! rows ([`crate::registry::WorkerRegistry`], [`crate::events::EventHub`]), or
//! a client to an external service ([`crate::artifacts::ArtifactService`]).

use std::sync::Arc;

use crate::artifacts::ArtifactService;
use crate::config::RemoteConfig;
use crate::db::Db;
use crate::events::EventHub;
use crate::registry::WorkerRegistry;
use crate::scheduler::SchedulerHandle;

/// Everything a request handler or background task needs. Cheap to clone:
/// every field is either an `Arc`, a connection pool handle, or itself
/// internally `Arc`-backed.
#[derive(Clone)]
pub struct AppState {
    /// Static configuration loaded at startup.
    pub config: Arc<RemoteConfig>,
    /// The `PostgreSQL` connection pool and credential hasher.
    pub db: Db,
    /// Live Generation event fanout for Native API subscribers.
    pub events: EventHub,
    /// Live Worker gRPC sessions.
    pub workers: WorkerRegistry,
    /// Artifact storage and transfer.
    pub artifacts: ArtifactService,
    /// Handle to wake the scheduler loop.
    pub scheduler: SchedulerHandle,
    /// Cancelled when Remote begins graceful shutdown, so indefinite Worker
    /// control streams end instead of blocking the drain (ADR 0020).
    pub shutdown: tokio_util::sync::CancellationToken,
}
