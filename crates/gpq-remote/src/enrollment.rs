//! Worker enrollment (ADR 0004, ADR 0009).
//!
//! One-time enrollment authenticated by the Tenant Master Key. Remote never
//! stores the Master Key itself, verifies it against the keyed hash on file,
//! rejects a protocol major mismatch outright, and issues a fresh, distinct
//! Worker Credential — returned exactly once in the response and persisted
//! only as its own keyed hash.

use connectrpc::{ConnectError, ErrorCode, Response, ServiceResult};
use gpq_proto::gpq::worker::v1::{EnrollRequest, EnrollResponse, WorkerEnrollmentService};

use crate::state::AppState;

/// Implements [`WorkerEnrollmentService`] against shared Remote state.
#[derive(Clone)]
pub struct EnrollmentApi {
    state: AppState,
}

impl EnrollmentApi {
    /// Builds the service over `state`.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

/// Maps a database failure to an internal Connect error; never surfaces raw
/// database detail that could leak schema information to a Worker.
fn internal(err: &sqlx::Error) -> ConnectError {
    tracing::error!(error = %err, "worker enrollment database operation failed");
    ConnectError::new(ErrorCode::Internal, "Internal error.")
}

fn internal_anyhow(err: &anyhow::Error) -> ConnectError {
    tracing::error!(error = %err, "worker enrollment failed");
    ConnectError::new(ErrorCode::Internal, "Internal error.")
}

/// `ServiceResult<T>` names a concrete response type at every call site
/// below, while `WorkerEnrollmentService`'s generated trait methods declare
/// an opaque `impl Encodable<T> + Send` return; that is a deliberate,
/// harmless refinement rustc's `refining_impl_trait` warns about only
/// because a generic caller could otherwise observe a narrower type than
/// the trait promises — impossible here since this is a binary crate (no
/// `lib.rs`) with no external consumer of `WorkerEnrollmentService` at all.
#[expect(
    refining_impl_trait_reachable,
    reason = "binary crate: WorkerEnrollmentService has no external caller that could observe the refinement"
)]
impl WorkerEnrollmentService for EnrollmentApi {
    async fn enroll(
        &self,
        ctx: connectrpc::RequestContext,
        request: connectrpc::ServiceRequest<'_, EnrollRequest>,
    ) -> ServiceResult<EnrollResponse> {
        let Some(token) = crate::auth::bearer_token(ctx.headers()) else {
            return Err(ConnectError::new(
                ErrorCode::Unauthenticated,
                "missing bearer token",
            ));
        };

        let tenant_id = self
            .state
            .db
            .authenticate_master_key(token)
            .await
            .map_err(|err| internal_anyhow(&err))?;
        let Some(tenant_id) = tenant_id else {
            return Err(ConnectError::new(
                ErrorCode::Unauthenticated,
                "invalid tenant master key",
            ));
        };

        let request = request.to_owned_message();

        // ADR 0004: a major protocol mismatch is rejected explicitly rather
        // than tolerated and later misbehaving.
        if !gpq_proto::protocol_compatible(request.protocol_major) {
            return Err(ConnectError::new(
                ErrorCode::FailedPrecondition,
                format!(
                    "worker protocol major {} is incompatible with remote major {}",
                    request.protocol_major,
                    gpq_proto::PROTOCOL_MAJOR
                ),
            ));
        }
        if request.worker_name.is_empty() {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "worker_name must not be empty",
            ));
        }

        // ADR 0009: generated fresh per enrollment, persisted only as a
        // keyed hash, and returned to the caller exactly once below. Never
        // logged.
        let credential = crate::auth::generate_secret("gpq_wc_");
        let credential_hash = self.state.db.hasher().hash(&credential);

        let mut tx = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(|err| internal(&err))?;
        let worker_id = crate::db::workers::enroll(
            &mut tx,
            tenant_id,
            crate::db::workers::WorkerEnrollment {
                name: &request.worker_name,
                host_descriptor: &request.host_descriptor,
                worker_version: &request.worker_version,
                protocol_major: request.protocol_major,
                protocol_minor: request.protocol_minor,
                credential_hash: &credential_hash,
            },
        )
        .await
        .map_err(|err| internal(&err))?;
        tx.commit().await.map_err(|err| internal(&err))?;

        Response::ok(EnrollResponse {
            worker_id: worker_id.to_string(),
            worker_credential: credential,
            tenant_id: tenant_id.to_string(),
            ..Default::default()
        })
    }
}
