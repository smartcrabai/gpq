//! `gpq-remote`: the GPU Generation Queue coordinator.
//!
//! Exposed as a library so the integration suites under `tests/` exercise the
//! real admission, scheduling, and `db` code paths instead of hand-copied
//! reproductions of their SQL, which silently drift from production (ADR
//! 0013's leasing invariants are only meaningful if the shipped query is the
//! one under test). The `gpq-remote` binary is a thin shim over
//! [`cli::run`].

pub mod admission;
pub mod artifacts;
pub mod auth;
pub mod cli;
pub mod config;
pub mod db;
pub mod enrollment;
pub mod events;
pub mod expiry;
pub mod http;
pub mod native;
pub mod openai;
pub mod registry;
pub mod scheduler;
pub mod session;
pub mod state;
pub mod telemetry;
pub mod tenant_console;
pub mod transfer;
