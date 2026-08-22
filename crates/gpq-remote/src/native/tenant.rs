//! Native Tenant settings service (ADR 0006).
//!
//! Queue age, capacity, Artifact limits, timeout ceilings, and default
//! priority are readable and mutable through this Master-Key-authenticated
//! service; credential and Tenant lifecycle operations stay in local
//! `gpq-remote` administration commands (ADR 0009).

use connectrpc::{ConnectError, ErrorCode, Response, ServiceRequest, ServiceResult};
use gpq_domain::TenantSettings;
use gpq_proto::gpq::v1::{
    GetTenantSettingsRequest, GetTenantSettingsResponse, TenantService,
    TenantSettings as WireTenantSettings, UpdateTenantSettingsRequest,
    UpdateTenantSettingsResponse,
};

use crate::state::AppState;

/// `TenantService` implementation backed by `db::tenants`.
pub struct TenantApi {
    state: AppState,
}

impl TenantApi {
    /// Builds the service over shared application state.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

/// Wire field names accepted in `UpdateTenantSettingsRequest.update_mask`,
/// matching `gpq/v1/tenant.proto` exactly.
mod mask_path {
    pub const MAXIMUM_QUEUE_AGE: &str = "maximum_queue_age";
    pub const MAX_QUEUED_GENERATIONS: &str = "max_queued_generations";
    pub const MAX_INPUT_ARTIFACT_BYTES: &str = "max_input_artifact_bytes";
    pub const MAX_OUTPUT_ARTIFACT_BYTES: &str = "max_output_artifact_bytes";
    pub const EXECUTION_TIMEOUT_CEILING: &str = "execution_timeout_ceiling";
    pub const DEFAULT_PRIORITY: &str = "default_priority";
}

fn tenant_settings_to_proto(settings: TenantSettings) -> WireTenantSettings {
    WireTenantSettings {
        maximum_queue_age: crate::native::duration_to_proto(settings.maximum_queue_age),
        max_queued_generations: settings.max_queued_generations,
        max_input_artifact_bytes: settings.max_input_artifact_bytes,
        max_output_artifact_bytes: settings.max_output_artifact_bytes,
        execution_timeout_ceiling: crate::native::duration_to_proto(
            settings.execution_timeout_ceiling,
        ),
        default_priority: u32::from(settings.default_priority.get()),
        ..Default::default()
    }
}

/// Applies `update_mask` onto `current`, taking each named field from
/// `incoming`. An empty mask replaces every field (per
/// `UpdateTenantSettingsRequest.update_mask`'s documented contract). Unknown
/// paths are rejected rather than silently ignored, so a caller typo surfaces
/// immediately instead of applying a no-op update.
fn apply_update_mask(
    current: WireTenantSettings,
    incoming: WireTenantSettings,
    update_mask: &[String],
) -> Result<WireTenantSettings, String> {
    if update_mask.is_empty() {
        return Ok(incoming);
    }
    let mut merged = current;
    for path in update_mask {
        match path.as_str() {
            mask_path::MAXIMUM_QUEUE_AGE => {
                merged.maximum_queue_age = incoming.maximum_queue_age.clone();
            }
            mask_path::MAX_QUEUED_GENERATIONS => {
                merged.max_queued_generations = incoming.max_queued_generations;
            }
            mask_path::MAX_INPUT_ARTIFACT_BYTES => {
                merged.max_input_artifact_bytes = incoming.max_input_artifact_bytes;
            }
            mask_path::MAX_OUTPUT_ARTIFACT_BYTES => {
                merged.max_output_artifact_bytes = incoming.max_output_artifact_bytes;
            }
            mask_path::EXECUTION_TIMEOUT_CEILING => {
                merged.execution_timeout_ceiling = incoming.execution_timeout_ceiling.clone();
            }
            mask_path::DEFAULT_PRIORITY => {
                merged.default_priority = incoming.default_priority;
            }
            other => return Err(format!("unknown update_mask path {other:?}")),
        }
    }
    Ok(merged)
}

/// Shortest allowed `execution_timeout_ceiling` (ADR 0006 caps every resolved
/// timeout; a ceiling below a minute would make most modalities inadmissible).
const MIN_EXECUTION_TIMEOUT_CEILING_SECS: u64 = 60;

/// Converts and validates a fully-resolved wire `TenantSettings` into its
/// domain form (ADR 0002, ADR 0006): every duration must be set and
/// positive, every limit must be positive, and the execution timeout ceiling
/// must be at least a minute.
fn tenant_settings_from_proto(settings: WireTenantSettings) -> Result<TenantSettings, String> {
    let maximum_queue_age = crate::native::duration_from_proto(settings.maximum_queue_age)
        .ok_or_else(|| "maximum_queue_age is required".to_owned())?;
    if maximum_queue_age.is_zero() {
        return Err("maximum_queue_age must be positive".to_owned());
    }
    let execution_timeout_ceiling =
        crate::native::duration_from_proto(settings.execution_timeout_ceiling)
            .ok_or_else(|| "execution_timeout_ceiling is required".to_owned())?;
    if execution_timeout_ceiling.as_secs() < MIN_EXECUTION_TIMEOUT_CEILING_SECS {
        return Err("execution_timeout_ceiling must be at least one minute".to_owned());
    }
    if settings.max_queued_generations == 0 {
        return Err("max_queued_generations must be positive".to_owned());
    }
    if settings.max_input_artifact_bytes == 0 {
        return Err("max_input_artifact_bytes must be positive".to_owned());
    }
    if settings.max_output_artifact_bytes == 0 {
        return Err("max_output_artifact_bytes must be positive".to_owned());
    }
    let default_priority = crate::native::priority_from_wire(settings.default_priority)?;
    Ok(TenantSettings {
        maximum_queue_age,
        max_queued_generations: settings.max_queued_generations,
        max_input_artifact_bytes: settings.max_input_artifact_bytes,
        max_output_artifact_bytes: settings.max_output_artifact_bytes,
        execution_timeout_ceiling,
        default_priority,
    })
}

/// `ServiceResult<T>` names a concrete response type at every call site
/// below, while `TenantService`'s generated trait methods declare an
/// opaque `impl Encodable<T> + Send` return; that is a deliberate, harmless
/// refinement rustc's `refining_impl_trait` warns about only because a
/// generic caller could otherwise observe a narrower type than the trait
/// promises — impossible here since this is a binary crate (no `lib.rs`)
/// with no external consumer of `TenantService` at all.
#[expect(
    refining_impl_trait_reachable,
    reason = "binary crate: TenantService has no external caller that could observe the refinement"
)]
impl TenantService for TenantApi {
    async fn get_tenant_settings(
        &self,
        ctx: connectrpc::RequestContext,
        _request: ServiceRequest<'_, GetTenantSettingsRequest>,
    ) -> ServiceResult<GetTenantSettingsResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(|err| ConnectError::new(ErrorCode::Internal, err.to_string()))?;
        let settings = crate::db::tenants::load_settings(&mut conn, tenant_id)
            .await
            .map_err(|err| ConnectError::new(ErrorCode::Internal, err.to_string()))?;
        conn.commit()
            .await
            .map_err(|err| ConnectError::new(ErrorCode::Internal, err.to_string()))?;
        Response::ok(GetTenantSettingsResponse {
            settings: tenant_settings_to_proto(settings).into(),
            ..Default::default()
        })
    }

    async fn update_tenant_settings(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, UpdateTenantSettingsRequest>,
    ) -> ServiceResult<UpdateTenantSettingsResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let incoming = request.settings.into_option().unwrap_or_default();
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(|err| ConnectError::new(ErrorCode::Internal, err.to_string()))?;
        let current = crate::db::tenants::load_settings(&mut conn, tenant_id)
            .await
            .map_err(|err| ConnectError::new(ErrorCode::Internal, err.to_string()))?;
        let current_proto = tenant_settings_to_proto(current);
        let merged_proto = apply_update_mask(current_proto, incoming, &request.update_mask)
            .map_err(|err| ConnectError::new(ErrorCode::InvalidArgument, err))?;
        let merged = tenant_settings_from_proto(merged_proto)
            .map_err(|err| ConnectError::new(ErrorCode::InvalidArgument, err))?;
        crate::db::tenants::update_settings(&mut conn, tenant_id, &merged)
            .await
            .map_err(|err| ConnectError::new(ErrorCode::Internal, err.to_string()))?;
        conn.commit()
            .await
            .map_err(|err| ConnectError::new(ErrorCode::Internal, err.to_string()))?;
        Response::ok(UpdateTenantSettingsResponse {
            settings: tenant_settings_to_proto(merged).into(),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gpq_domain::Priority;

    use super::*;

    fn sample() -> TenantSettings {
        TenantSettings {
            maximum_queue_age: Duration::from_mins(30),
            max_queued_generations: 1_000,
            max_input_artifact_bytes: 256 * 1024 * 1024,
            max_output_artifact_bytes: 2 * 1024 * 1024 * 1024,
            execution_timeout_ceiling: Duration::from_hours(24),
            default_priority: Priority::DEFAULT,
        }
    }

    #[test]
    fn settings_round_trip_through_proto() {
        let settings = sample();
        let proto = tenant_settings_to_proto(settings);
        let Ok(back) = tenant_settings_from_proto(proto) else {
            panic!("valid settings decode");
        };
        assert_eq!(back, sample());
    }

    #[test]
    fn empty_mask_replaces_every_field() {
        let current = tenant_settings_to_proto(sample());
        let mut incoming_settings = sample();
        incoming_settings.max_queued_generations = 42;
        let incoming = tenant_settings_to_proto(incoming_settings);
        let Ok(merged) = apply_update_mask(current, incoming, &[]) else {
            panic!("empty mask always applies");
        };
        assert_eq!(merged.max_queued_generations, 42);
    }

    #[test]
    fn partial_mask_touches_only_named_fields() {
        let current = tenant_settings_to_proto(sample());
        let mut incoming_settings = sample();
        incoming_settings.max_queued_generations = 42;
        incoming_settings.default_priority = Priority::MAX;
        let incoming = tenant_settings_to_proto(incoming_settings);
        let mask = vec!["max_queued_generations".to_owned()];
        let Ok(merged) = apply_update_mask(current, incoming, &mask) else {
            panic!("known field");
        };
        assert_eq!(merged.max_queued_generations, 42);
        // default_priority was not in the mask, so it keeps the current value.
        assert_eq!(merged.default_priority, u32::from(Priority::DEFAULT.get()));
    }

    #[test]
    fn unknown_mask_path_is_rejected() {
        let current = tenant_settings_to_proto(sample());
        let incoming = tenant_settings_to_proto(sample());
        let mask = vec!["not_a_real_field".to_owned()];
        assert!(apply_update_mask(current, incoming, &mask).is_err());
    }

    #[test]
    fn zero_duration_is_rejected() {
        let mut settings = tenant_settings_to_proto(sample());
        settings.maximum_queue_age = crate::native::duration_to_proto(Duration::ZERO);
        assert!(tenant_settings_from_proto(settings).is_err());
    }

    #[test]
    fn execution_timeout_ceiling_below_one_minute_is_rejected() {
        let mut settings = tenant_settings_to_proto(sample());
        settings.execution_timeout_ceiling =
            crate::native::duration_to_proto(Duration::from_secs(30));
        assert!(tenant_settings_from_proto(settings).is_err());
    }

    #[test]
    fn default_priority_out_of_range_is_rejected() {
        let mut settings = tenant_settings_to_proto(sample());
        settings.default_priority = 10;
        assert!(tenant_settings_from_proto(settings).is_err());
    }
}
