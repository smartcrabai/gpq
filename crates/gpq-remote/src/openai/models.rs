//! `GET /v1/models` (ADR 0006, ADR 0012).
//!
//! `OpenAI` model listing exposes Model *aliases* only, never Workflow aliases
//! and never Model Version detail; Native APIs are the place for online
//! availability and capability detail (ADR 0012).

use axum::Json;
use axum::extract::State;
use serde::Serialize;

use super::{ApiError, TenantAuth};
use crate::state::AppState;

/// One entry of `GET /v1/models`.
#[derive(Debug, Serialize)]
pub struct ModelObject {
    /// The Model alias.
    pub id: String,
    /// Always `"model"`.
    pub object: &'static str,
    /// Unix seconds the alias was created.
    pub created: i64,
    /// Static attribution; GPQ does not track a per-alias owner.
    pub owned_by: &'static str,
}

/// The `GET /v1/models` response envelope.
#[derive(Debug, Serialize)]
pub struct ModelListResponse {
    /// Always `"list"`.
    pub object: &'static str,
    /// One entry per Model alias.
    pub data: Vec<ModelObject>,
}

/// Lists the authenticated Tenant's Model aliases.
pub async fn list_models(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
) -> Result<Json<ModelListResponse>, ApiError> {
    let mut tx = state.db.begin_tenant(tenant_id).await.map_err(|err| {
        tracing::error!(error = %err, "failed to begin tenant transaction");
        ApiError::internal("Internal error.")
    })?;
    let aliases = crate::db::catalog::list_model_aliases(&mut tx, tenant_id)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to list model aliases");
            ApiError::internal("Internal error.")
        })?;
    let data = aliases
        .into_iter()
        .map(|row| ModelObject {
            id: row.alias,
            object: "model",
            created: row.created_at.timestamp(),
            owned_by: "gpq",
        })
        .collect();
    Ok(Json(ModelListResponse {
        object: "list",
        data,
    }))
}
