//! Assembles the single Axum router that serves every public surface on one
//! port.
//!
//! ADR 0004: OpenAI-compatible routes, one-shot Artifact download, and health
//! checks are Axum; Native Generation/Catalog/Tenant use Connect, and Worker
//! enrollment/Session/transfer use gRPC — but `connectrpc`'s content-type
//! negotiation lets all of them share this one `axum::serve` listener (ADR
//! 0019: plaintext h2c/HTTP-1.1 behind an ingress that terminates TLS).

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;

use crate::state::AppState;

/// Builds the fully assembled, state-erased router served by `gpq-remote serve`.
#[must_use = "the router does nothing until served with axum::serve"]
pub fn router(state: AppState) -> axum::Router {
    let health = axum::Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state.clone());

    let worker_and_native_rpc = connectrpc::Router::new()
        .add_service(Arc::new(crate::native::GenerationApi::new(state.clone())))
        .add_service(Arc::new(crate::native::CatalogApi::new(state.clone())))
        .add_service(Arc::new(crate::native::TenantApi::new(state.clone())))
        .add_service(Arc::new(crate::enrollment::EnrollmentApi::new(
            state.clone(),
        )))
        .add_service(Arc::new(crate::session::SessionApi::new(state.clone())))
        .add_service(Arc::new(crate::transfer::TransferApi::new(state.clone())))
        .into_axum_router();

    health
        .merge(crate::tenant_console::router())
        .merge(crate::openai::router(state.clone()))
        .merge(crate::artifacts::download_router(state))
        .merge(worker_and_native_rpc)
        .layer(tower_http::trace::TraceLayer::new_for_http())
}

/// Liveness: the process is up and serving HTTP. Never depends on
/// `PostgreSQL` or S3 so an orchestrator does not restart a Remote instance
/// that is merely waiting on a slow dependency.
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Readiness: `PostgreSQL` is reachable. Deliberately never checks the object
/// store — S3 configuration is optional and never affects readiness
/// (ADR 0008); a Tenant relying on S3-backed features simply gets an
/// explicit admission failure for those specific requests instead.
///
/// Once `AppState::shutdown` is cancelled this reports unready immediately,
/// without touching `PostgreSQL`, so an ingress stops routing new traffic —
/// including new Worker enrollment/`Session` attempts — while the bounded
/// drain in `main::run_serve` is under way.
async fn readyz(State(state): State<AppState>) -> StatusCode {
    if state.shutdown.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match sqlx::query("SELECT 1").execute(state.db.pool()).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
