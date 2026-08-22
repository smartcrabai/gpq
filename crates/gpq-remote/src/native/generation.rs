//! Native Generation service (ADR 0006).
//!
//! `Submit` resolves exactly one Model or Workflow alias through
//! `crate::admission::admit`, the same acceptance path the OpenAI-compatible
//! surface uses, tagged `CallerKind::Durable` so it survives disconnection
//! and Remote restarts (ADR 0003). `GetGeneration`, `ListGenerations`, and
//! `CancelGeneration` read and mutate the persisted Generation directly.
//! `WatchGeneration` starts from the current snapshot and then streams live
//! events with nothing replayed: token deltas never repeat, and a missed
//! span of live events surfaces as an explicit `Discontinuity` (ADR 0006).
//! `CreateInputArtifact` provisions an object-store upload slot for a queued
//! Native Generation's input (ADR 0008).

use std::str::FromStr;

use buffa::MessageField;
use buffa_types::google::protobuf::Timestamp as ProtoTimestamp;
use chrono::{DateTime, Utc};
use connectrpc::{ConnectError, ErrorCode, Response, ServiceRequest, ServiceResult, ServiceStream};
use futures::StreamExt;
use gpq_domain::{ArtifactId, CallerKind, ContentHash, GenerationId, TenantId};
use gpq_proto::gpq::v1::{
    ArtifactManifest as WireArtifactManifest, ArtifactRef, CancelGenerationRequest,
    CancelGenerationResponse, CreateInputArtifactRequest, CreateInputArtifactResponse,
    Discontinuity, Failure, Generation, GenerationEvent as WireGenerationEvent, GenerationService,
    GetGenerationRequest, GetGenerationResponse, ListGenerationsRequest, ListGenerationsResponse,
    Progress, StateChanged, SubmitRequest, SubmitResponse, TokenDelta, Usage,
    WatchGenerationRequest, generation_event, submit_request,
};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::admission::{AdmissionError, AdmissionRequest, AliasTarget};
use crate::db::artifacts::ArtifactRow;
use crate::db::generations::GenerationRow;
use crate::events::GenerationEvent;
use crate::state::AppState;

/// A Generation list page larger than the caller's request but small enough
/// to keep one response bounded.
const DEFAULT_PAGE_SIZE: u32 = 50;
/// Hard ceiling on `ListGenerationsRequest.page_size`.
const MAX_PAGE_SIZE: u32 = 200;
/// Header carrying the idempotency key for `Submit` (ADR 0006: "idempotency
/// travels in request metadata", i.e. transport headers rather than a
/// message field).
const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
/// Upper bound on `Submit`'s idempotency key length: generous enough for any
/// UUID, request hash, or correlation id a caller would reasonably send,
/// while bounding the row Remote persists per key in `idempotency_keys`.
const MAX_IDEMPOTENCY_KEY_LEN: usize = 200;

/// `GenerationService` implementation backed by `crate::admission` and
/// `db::generations`.
pub struct GenerationApi {
    state: AppState,
}

impl GenerationApi {
    /// Builds the service over shared application state.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

fn internal(err: impl std::fmt::Display) -> ConnectError {
    ConnectError::new(ErrorCode::Internal, err.to_string())
}

fn invalid(err: impl std::fmt::Display) -> ConnectError {
    ConnectError::new(ErrorCode::InvalidArgument, err.to_string())
}

/// Validates and extracts `Submit`'s idempotency key from the
/// `idempotency-key` header (ADR 0006: Native creation requires one, unlike
/// the optional key `OpenAI` endpoints accept). Rejects a missing, blank, or
/// oversized key before it reaches `crate::admission::admit`, so a caller
/// that forgets — or whose retry drops — the header cannot silently
/// duplicate a Generation and burn duplicate GPU time.
fn required_idempotency_key(header: Option<&str>) -> Result<String, ConnectError> {
    let key = header
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| invalid("idempotency-key header is required for Submit"))?;
    if key.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(invalid(format!(
            "idempotency-key must be at most {MAX_IDEMPOTENCY_KEY_LEN} bytes"
        )));
    }
    Ok(key.to_owned())
}

/// Maps `crate::admission::AdmissionError` to the Connect codes the parent
/// contract fixes: `UnknownAlias` -> `NotFound`, `InvalidInput` ->
/// `InvalidArgument`, `CapacityExceeded` -> `ResourceExhausted`,
/// `ObjectStoreUnavailable` -> `FailedPrecondition`, `Unavailable` ->
/// `Unavailable`.
fn admission_error(err: AdmissionError) -> ConnectError {
    match err {
        AdmissionError::UnknownAlias => ConnectError::new(ErrorCode::NotFound, "alias not found"),
        AdmissionError::InvalidInput(message) => {
            ConnectError::new(ErrorCode::InvalidArgument, message)
        }
        AdmissionError::CapacityExceeded => ConnectError::new(
            ErrorCode::ResourceExhausted,
            "tenant queue capacity exceeded",
        ),
        AdmissionError::ObjectStoreUnavailable => ConnectError::new(
            ErrorCode::FailedPrecondition,
            "object storage is not configured",
        ),
        AdmissionError::Unavailable => {
            ConnectError::new(ErrorCode::Unavailable, "no capable Worker is online")
        }
        AdmissionError::Internal(err) => internal(err),
    }
}

#[derive(serde::Deserialize)]
struct UsageJson {
    #[serde(rename = "prompt_tokens")]
    prompt: u32,
    #[serde(rename = "completion_tokens")]
    completion: u32,
    #[serde(rename = "total_tokens")]
    total: u32,
}

fn usage_from_json(value: serde_json::Value) -> Result<Usage, serde_json::Error> {
    let parsed: UsageJson = serde_json::from_value(value)?;
    Ok(Usage {
        prompt_tokens: parsed.prompt,
        completion_tokens: parsed.completion,
        total_tokens: parsed.total,
        ..Default::default()
    })
}

#[derive(serde::Deserialize)]
struct ProgressJson {
    fraction: f64,
    stage: String,
    step: u32,
    total_steps: u32,
    observed_at: DateTime<Utc>,
}

fn progress_from_json(value: serde_json::Value) -> Result<Progress, serde_json::Error> {
    let parsed: ProgressJson = serde_json::from_value(value)?;
    Ok(Progress {
        fraction: parsed.fraction,
        stage: parsed.stage,
        step: parsed.step,
        total_steps: parsed.total_steps,
        observed_at: crate::native::timestamp_to_proto(parsed.observed_at),
        ..Default::default()
    })
}

/// Builds one output Artifact's wire reference, with `download_path` set to
/// the absolute URL a client can `GET` (`RemoteConfig::public_base_url` +
/// [`crate::artifacts::download_url`]), not a bare route.
fn artifact_row_to_proto(
    public_base_url: &url::Url,
    artifact: &ArtifactRow,
) -> Result<ArtifactRef, ConnectError> {
    let download_path = crate::artifacts::download_url(public_base_url, artifact.id)
        .map_err(internal)?
        .to_string();
    Ok(ArtifactRef {
        artifact_id: artifact.id.as_uuid().to_string(),
        manifest: WireArtifactManifest {
            size_bytes: artifact.manifest.size_bytes,
            digest_sha256: artifact.manifest.digest.to_hex(),
            kind: crate::native::media_kind_to_proto(artifact.manifest.kind),
            mime_type: artifact.manifest.mime_type.clone(),
            ..Default::default()
        }
        .into(),
        placement: crate::native::artifact_placement_to_proto(artifact.placement),
        state: crate::native::artifact_state_to_proto(artifact.state),
        download_path,
        ..Default::default()
    })
}

/// Builds the wire `Generation` for one persisted row, including its output
/// Artifacts (ADR 0006, ADR 0008).
async fn generation_row_to_proto(
    state: &AppState,
    tenant_id: TenantId,
    row: &GenerationRow,
) -> Result<Generation, ConnectError> {
    let generation_id = GenerationId::from_uuid(row.id);
    let mut conn = state.db.begin_tenant(tenant_id).await.map_err(internal)?;
    let outputs = crate::db::artifacts::list_outputs(&mut conn, tenant_id, generation_id)
        .await
        .map_err(internal)?;
    conn.commit().await.map_err(internal)?;

    let generation_state = gpq_domain::GenerationState::from_str(&row.state).map_err(internal)?;
    let modality = gpq_domain::Modality::from_str(&row.modality).map_err(internal)?;
    let execution_timeout = crate::db::catalog::interval_to_duration(
        "generations.execution_timeout",
        row.execution_timeout,
    )
    .map_err(internal)?;

    let output_artifacts = outputs
        .iter()
        .map(|artifact| artifact_row_to_proto(&state.config.public_base_url, artifact))
        .collect::<Result<Vec<_>, _>>()?;

    let usage = row
        .usage
        .clone()
        .map(usage_from_json)
        .transpose()
        .map_err(internal)?;
    let failure = row
        .failure_kind
        .as_deref()
        .map(|kind| {
            let kind = gpq_domain::FailureKind::from_str(kind).map_err(internal)?;
            Ok::<_, ConnectError>(Failure {
                kind: crate::native::failure_kind_to_proto(kind),
                message: row.failure_message.clone(),
                // Not persisted on `generations` (only per-Attempt); the
                // classified `kind` above is what Remote's retry policy
                // actually acted on.
                worker_retry_hint: false,
                ..Default::default()
            })
        })
        .transpose()?;
    let latest_progress = row
        .latest_progress
        .clone()
        .map(progress_from_json)
        .transpose()
        .map_err(internal)?;

    Ok(Generation {
        generation_id: row.id.to_string(),
        state: crate::native::generation_state_to_proto(generation_state),
        modality: crate::native::modality_to_proto(modality),
        alias: row.alias.clone(),
        version_sha256: row.version_sha256.clone(),
        priority: u32::try_from(row.priority).unwrap_or(0),
        seed: row
            .seed
            .and_then(|seed| u64::try_from(seed).ok())
            .unwrap_or(0),
        execution_timeout: crate::native::duration_to_proto(execution_timeout),
        created_at: crate::native::timestamp_to_proto(row.created_at),
        updated_at: crate::native::timestamp_to_proto(row.updated_at),
        attempt_count: u32::try_from(row.attempt_count).unwrap_or(0),
        output_text: row.output_text.clone(),
        output_artifacts,
        usage: usage.into(),
        failure: failure.into(),
        latest_progress: latest_progress.into(),
        ..Default::default()
    })
}

/// Converts one live or replayed `GenerationEvent` to its wire
/// representation, stamped with `emitted_at` (the current time for a live
/// event, or the persisted row's `created_at` for a replayed one — so a
/// replay reports when the event actually happened, not when it was
/// replayed). `GenerationEvent::Output` has no wire counterpart in
/// `generation_event::Event` (`snapshot`/`state_changed`/`progress`/
/// `token_delta`/`discontinuity` only), so it maps to `None` here; the live
/// `watch_generation` stream instead intercepts it before calling this
/// function and builds a fresh `Snapshot` event
/// ([`refreshed_snapshot_event`]) so a watcher still learns the output
/// Artifact's download path (ADR 0006, ADR 0008).
fn domain_event_to_proto(
    event: GenerationEvent,
    emitted_at: MessageField<ProtoTimestamp>,
) -> Option<WireGenerationEvent> {
    let wire_event = match event {
        GenerationEvent::State {
            state,
            attempt_count,
            failure,
        } => generation_event::Event::StateChanged(Box::new(StateChanged {
            state: crate::native::generation_state_to_proto(state),
            attempt_count,
            failure: failure
                .map(|(kind, message)| Failure {
                    kind: crate::native::failure_kind_to_proto(kind),
                    message,
                    worker_retry_hint: false,
                    ..Default::default()
                })
                .into(),
            ..Default::default()
        })),
        GenerationEvent::Progress {
            fraction,
            stage,
            step,
            total_steps,
        } => generation_event::Event::Progress(Box::new(Progress {
            fraction,
            stage,
            step,
            total_steps,
            observed_at: emitted_at.clone(),
            ..Default::default()
        })),
        GenerationEvent::Token { text } => {
            generation_event::Event::TokenDelta(Box::new(TokenDelta {
                text,
                ..Default::default()
            }))
        }
        GenerationEvent::Output => return None,
        GenerationEvent::Discontinuity { reason } => {
            generation_event::Event::Discontinuity(Box::new(Discontinuity {
                reason,
                ..Default::default()
            }))
        }
    };
    Some(WireGenerationEvent {
        emitted_at,
        event: Some(wire_event),
        ..Default::default()
    })
}

fn discontinuity_event(reason: &str) -> WireGenerationEvent {
    WireGenerationEvent {
        emitted_at: crate::native::timestamp_to_proto(Utc::now()),
        event: Some(generation_event::Event::Discontinuity(Box::new(
            Discontinuity {
                reason: reason.to_owned(),
                ..Default::default()
            },
        ))),
        ..Default::default()
    }
}

/// Converts persisted rows already filtered to `sequence > snapshot_sequence`
/// (`crate::db::events::load_since`) into their wire events, in order.
/// `attempt_created` rows and any row whose payload fails to parse are
/// silently dropped: `attempt_created` has no `GenerationEvent` counterpart
/// (ADR 0008 keeps it purely as audit history) and a `WatchGeneration`
/// replay never surfaces it.
fn replay_events(rows: &[crate::db::events::EventRow]) -> Vec<WireGenerationEvent> {
    rows.iter()
        .filter_map(|row| {
            let event = crate::events::decode_persisted(row)?;
            domain_event_to_proto(event, crate::native::timestamp_to_proto(row.created_at))
        })
        .collect()
}

/// Re-reads the Generation row and rebuilds its wire snapshot, wrapped as a
/// `Snapshot` event. This is the only way a `WatchGeneration` caller learns
/// an output Artifact's download path: `GenerationEvent::Output` has no
/// direct wire representation, so `watch_generation` calls this instead of
/// `domain_event_to_proto` whenever one arrives live (ADR 0006, ADR 0008).
async fn refreshed_snapshot_event(
    state: &AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
) -> Result<WireGenerationEvent, ConnectError> {
    let mut conn = state.db.begin_tenant(tenant_id).await.map_err(internal)?;
    let Some(row) = crate::db::generations::get(&mut conn, tenant_id, generation_id)
        .await
        .map_err(internal)?
    else {
        return Err(ConnectError::new(
            ErrorCode::NotFound,
            "generation not found",
        ));
    };
    conn.commit().await.map_err(internal)?;
    let snapshot = generation_row_to_proto(state, tenant_id, &row).await?;
    Ok(WireGenerationEvent {
        emitted_at: crate::native::timestamp_to_proto(Utc::now()),
        event: Some(generation_event::Event::Snapshot(Box::new(snapshot))),
        ..Default::default()
    })
}

/// Resolves `ListGenerationsRequest.page_size` to an effective, bounded page
/// size: `0` (unset) becomes [`DEFAULT_PAGE_SIZE`], and anything above
/// [`MAX_PAGE_SIZE`] is clamped down to it.
fn effective_page_size(requested: u32) -> u32 {
    if requested == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        requested.min(MAX_PAGE_SIZE)
    }
}

/// `ServiceResult<T>` names a concrete response type at every call site
/// below, while `GenerationService`'s generated trait methods declare an
/// opaque `impl Encodable<T> + Send` return; that is a deliberate, harmless
/// refinement rustc's `refining_impl_trait` warns about only because a
/// generic caller could otherwise observe a narrower type than the trait
/// promises — impossible here since this is a binary crate (no `lib.rs`)
/// with no external consumer of `GenerationService` at all.
#[expect(
    refining_impl_trait_reachable,
    reason = "binary crate: GenerationService has no external caller that could observe the refinement"
)]
impl GenerationService for GenerationApi {
    async fn submit(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, SubmitRequest>,
    ) -> ServiceResult<SubmitResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let idempotency_key = required_idempotency_key(
            ctx.headers()
                .get(IDEMPOTENCY_KEY_HEADER)
                .and_then(|value| value.to_str().ok()),
        )?;
        let request = request.to_owned_message();

        let alias_target = match request.target {
            Some(submit_request::Target::ModelAlias(alias)) => AliasTarget::Model(alias),
            Some(submit_request::Target::WorkflowAlias(alias)) => AliasTarget::Workflow(alias),
            None => {
                return Err(invalid(
                    "exactly one of model_alias or workflow_alias is required",
                ));
            }
        };
        let parameters = request
            .parameters
            .into_option()
            .map(|value| serde_json::to_value(&value))
            .transpose()
            .map_err(|err| invalid(format!("invalid parameters: {err}")))?
            .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
        let mut input_artifact_ids = Vec::with_capacity(request.input_artifact_ids.len());
        for id in &request.input_artifact_ids {
            let artifact_id = ArtifactId::from_str(id)
                .map_err(|err| invalid(format!("invalid input_artifact_id {id:?}: {err}")))?;
            input_artifact_ids.push(artifact_id);
        }
        let output_placement =
            crate::native::artifact_placement_from_proto(request.output_placement)
                .ok_or_else(|| invalid("output_placement must be set"))?;
        let priority = (request.priority != 0)
            .then(|| crate::native::priority_from_wire(request.priority))
            .transpose()
            .map_err(invalid)?;
        let execution_timeout = crate::native::duration_from_proto(request.execution_timeout);

        let admission_request = AdmissionRequest {
            alias_target,
            parameters,
            input_artifact_ids,
            output_placement,
            priority,
            seed: request.seed,
            execution_timeout,
            caller_kind: CallerKind::Durable,
            // Native watchers may subscribe to live token deltas through
            // `WatchGeneration`; unlike OpenAI streaming this is not
            // requested per-call, so it is always on for Native submissions.
            stream_tokens: true,
            idempotency_key: Some(idempotency_key),
        };
        let row = crate::admission::admit(&self.state, tenant_id, admission_request)
            .await
            .map_err(admission_error)?;
        let generation = generation_row_to_proto(&self.state, tenant_id, &row).await?;
        Response::ok(SubmitResponse {
            generation: generation.into(),
            ..Default::default()
        })
    }

    async fn get_generation(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, GetGenerationRequest>,
    ) -> ServiceResult<GetGenerationResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let generation_id = GenerationId::from_str(&request.generation_id)
            .map_err(|err| invalid(format!("invalid generation_id: {err}")))?;
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let Some(row) = crate::db::generations::get(&mut conn, tenant_id, generation_id)
            .await
            .map_err(internal)?
        else {
            return Err(ConnectError::new(
                ErrorCode::NotFound,
                "generation not found",
            ));
        };
        conn.commit().await.map_err(internal)?;
        let generation = generation_row_to_proto(&self.state, tenant_id, &row).await?;
        Response::ok(GetGenerationResponse {
            generation: generation.into(),
            ..Default::default()
        })
    }

    async fn cancel_generation(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, CancelGenerationRequest>,
    ) -> ServiceResult<CancelGenerationResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let generation_id = GenerationId::from_str(&request.generation_id)
            .map_err(|err| invalid(format!("invalid generation_id: {err}")))?;
        let now = self.state.db.now().await.map_err(internal)?;
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        crate::db::generations::cancel_queued(&mut conn, tenant_id, generation_id, now)
            .await
            .map_err(internal)?;
        crate::db::generations::request_cancel_running(&mut conn, tenant_id, generation_id, now)
            .await
            .map_err(internal)?;
        let Some(row) = crate::db::generations::get(&mut conn, tenant_id, generation_id)
            .await
            .map_err(internal)?
        else {
            return Err(ConnectError::new(
                ErrorCode::NotFound,
                "generation not found",
            ));
        };
        conn.commit().await.map_err(internal)?;
        let generation = generation_row_to_proto(&self.state, tenant_id, &row).await?;
        Response::ok(CancelGenerationResponse {
            generation: generation.into(),
            ..Default::default()
        })
    }

    async fn list_generations(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, ListGenerationsRequest>,
    ) -> ServiceResult<ListGenerationsResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let page_size = effective_page_size(request.page_size);
        let after = if request.page_token.is_empty() {
            None
        } else {
            Some(
                GenerationId::from_str(&request.page_token)
                    .map_err(|err| invalid(format!("invalid page_token: {err}")))?,
            )
        };
        let state_filter = crate::native::generation_state_from_proto(request.state);

        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let rows =
            crate::db::generations::list(&mut conn, tenant_id, page_size, after, state_filter)
                .await
                .map_err(internal)?;
        conn.commit().await.map_err(internal)?;

        let next_page_token = if rows.len() == page_size as usize {
            rows.last()
                .map(|row| row.id.to_string())
                .unwrap_or_default()
        } else {
            String::new()
        };
        let mut generations = Vec::with_capacity(rows.len());
        for row in &rows {
            generations.push(generation_row_to_proto(&self.state, tenant_id, row).await?);
        }
        Response::ok(ListGenerationsResponse {
            generations,
            next_page_token,
            ..Default::default()
        })
    }

    async fn watch_generation(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, WatchGenerationRequest>,
    ) -> ServiceResult<ServiceStream<WireGenerationEvent>> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let generation_id = GenerationId::from_str(&request.generation_id)
            .map_err(|err| invalid(format!("invalid generation_id: {err}")))?;

        // Subscribed before any read so a state/progress transition
        // committed while the snapshot below is still being built (which
        // itself does a further DB round trip for output Artifacts) is
        // never lost: it either falls inside the `snapshot_sequence`
        // replay window below or is delivered live once the broadcast is
        // drained (ADR 0006: no gaps). The trade-off is a possible
        // duplicate delivery right at that boundary, harmless for
        // idempotent state/progress snapshots.
        let live = self.state.events.subscribe(generation_id);

        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let Some(row) = crate::db::generations::get(&mut conn, tenant_id, generation_id)
            .await
            .map_err(internal)?
        else {
            return Err(ConnectError::new(
                ErrorCode::NotFound,
                "generation not found",
            ));
        };
        let snapshot_sequence = crate::db::events::latest(&mut conn, tenant_id, generation_id)
            .await
            .map_err(internal)?
            .map_or(0, |latest| latest.sequence);
        conn.commit().await.map_err(internal)?;
        let snapshot = generation_row_to_proto(&self.state, tenant_id, &row).await?;
        let snapshot_event = WireGenerationEvent {
            emitted_at: crate::native::timestamp_to_proto(Utc::now()),
            event: Some(generation_event::Event::Snapshot(Box::new(snapshot))),
            ..Default::default()
        };

        let mut replay_conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let replay_rows = crate::db::events::load_since(
            &mut replay_conn,
            tenant_id,
            generation_id,
            snapshot_sequence,
        )
        .await
        .map_err(internal)?;
        replay_conn.commit().await.map_err(internal)?;
        let replay = replay_events(&replay_rows);

        let state = self.state.clone();
        let live_stream = BroadcastStream::new(live).filter_map(move |item| {
            let state = state.clone();
            async move {
                match item {
                    Ok(GenerationEvent::Output) => {
                        Some(refreshed_snapshot_event(&state, tenant_id, generation_id).await)
                    }
                    Ok(event) => {
                        domain_event_to_proto(event, crate::native::timestamp_to_proto(Utc::now()))
                            .map(Ok::<_, ConnectError>)
                    }
                    Err(BroadcastStreamRecvError::Lagged(_)) => Some(Ok(discontinuity_event(
                        "missed live events while reconnecting",
                    ))),
                }
            }
        });
        let stream = futures::stream::once(async move { Ok::<_, ConnectError>(snapshot_event) })
            .chain(futures::stream::iter(replay.into_iter().map(Ok)))
            .chain(live_stream);
        Response::stream_ok(stream)
    }

    async fn create_input_artifact(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, CreateInputArtifactRequest>,
    ) -> ServiceResult<CreateInputArtifactResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let wire_manifest = request
            .manifest
            .into_option()
            .ok_or_else(|| invalid("manifest is required"))?;
        let digest = ContentHash::from_str(&wire_manifest.digest_sha256)
            .map_err(|err| invalid(format!("invalid digest_sha256: {err}")))?;
        let kind = crate::native::media_kind_from_proto(wire_manifest.kind)
            .ok_or_else(|| invalid("manifest.kind must be set"))?;
        let manifest = gpq_domain::ArtifactManifest {
            size_bytes: wire_manifest.size_bytes,
            digest,
            kind,
            mime_type: wire_manifest.mime_type,
        };

        if !self.state.artifacts.object_store_available() {
            return Err(ConnectError::new(
                ErrorCode::FailedPrecondition,
                "object storage is not configured",
            ));
        }
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let settings = crate::db::tenants::load_settings(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        if !manifest.fits_within(settings.max_input_artifact_bytes) {
            return Err(invalid(format!(
                "input artifact of {} bytes exceeds the tenant limit of {} bytes",
                manifest.size_bytes, settings.max_input_artifact_bytes
            )));
        }

        let object_key = format!("tenants/{tenant_id}/inputs/{}", uuid::Uuid::now_v7());
        let (upload_url, expires_at) = self
            .state
            .artifacts
            .presign_put(&object_key, &manifest)
            .await
            .map_err(internal)?;
        let row = crate::db::artifacts::create_input(
            &mut conn,
            tenant_id,
            &manifest,
            gpq_domain::ArtifactPlacement::ObjectStore,
            Some(object_key.as_str()),
        )
        .await
        .map_err(internal)?;
        conn.commit().await.map_err(internal)?;

        Response::ok(CreateInputArtifactResponse {
            artifact_id: row.id.as_uuid().to_string(),
            upload_url: upload_url.to_string(),
            upload_url_expires_at: crate::native::timestamp_to_proto(expires_at),
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use gpq_domain::{FailureKind, GenerationState};

    use super::*;

    #[test]
    fn usage_json_round_trips() {
        let value =
            serde_json::json!({ "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 });
        let Ok(usage) = usage_from_json(value) else {
            panic!("valid usage json");
        };
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 5);
        assert_eq!(usage.total_tokens, 15);
    }

    #[test]
    fn required_idempotency_key_rejects_missing_header() {
        assert!(required_idempotency_key(None).is_err());
    }

    #[test]
    fn required_idempotency_key_rejects_blank_header() {
        assert!(required_idempotency_key(Some("   ")).is_err());
    }

    #[test]
    fn required_idempotency_key_rejects_oversized_header() {
        let key = "a".repeat(MAX_IDEMPOTENCY_KEY_LEN + 1);
        assert!(required_idempotency_key(Some(&key)).is_err());
    }

    #[test]
    fn required_idempotency_key_trims_and_accepts_a_reasonable_key() {
        let Ok(key) = required_idempotency_key(Some("  retry-42  ")) else {
            panic!("expected a valid idempotency key");
        };
        assert_eq!(key, "retry-42");
    }

    #[test]
    fn usage_json_rejects_missing_fields() {
        let value = serde_json::json!({ "prompt_tokens": 10 });
        assert!(usage_from_json(value).is_err());
    }

    #[test]
    fn progress_json_round_trips() {
        let value = serde_json::json!({
            "fraction": 0.5,
            "stage": "denoise",
            "step": 10,
            "total_steps": 20,
            "observed_at": "2024-01-01T00:00:00Z",
        });
        let Ok(progress) = progress_from_json(value) else {
            panic!("valid progress json");
        };
        assert!((progress.fraction - 0.5).abs() < f64::EPSILON);
        assert_eq!(progress.stage, "denoise");
        assert_eq!(progress.step, 10);
        assert_eq!(progress.total_steps, 20);
    }

    #[test]
    fn output_events_have_no_wire_representation() {
        assert!(
            domain_event_to_proto(
                GenerationEvent::Output,
                crate::native::timestamp_to_proto(Utc::now())
            )
            .is_none()
        );
    }

    #[test]
    fn state_events_map_to_state_changed() {
        let event = GenerationEvent::State {
            state: GenerationState::Failed,
            attempt_count: 2,
            failure: Some((FailureKind::OutOfMemory, "oom".to_owned())),
        };
        let Some(wire) =
            domain_event_to_proto(event, crate::native::timestamp_to_proto(Utc::now()))
        else {
            panic!("state event has a wire representation");
        };
        let Some(generation_event::Event::StateChanged(state_changed)) = wire.event else {
            panic!("expected StateChanged");
        };
        assert_eq!(
            crate::native::generation_state_from_proto(state_changed.state),
            Some(GenerationState::Failed)
        );
        assert_eq!(state_changed.attempt_count, 2);
        let Some(failure) = state_changed.failure.into_option() else {
            panic!("expected a failure payload");
        };
        assert_eq!(failure.message, "oom");
    }

    #[test]
    fn token_events_map_to_token_delta() {
        let event = GenerationEvent::Token {
            text: "hi".to_owned(),
        };
        let Some(wire) =
            domain_event_to_proto(event, crate::native::timestamp_to_proto(Utc::now()))
        else {
            panic!("token event has a wire representation");
        };
        let Some(generation_event::Event::TokenDelta(delta)) = wire.event else {
            panic!("expected TokenDelta");
        };
        assert_eq!(delta.text, "hi");
    }

    #[test]
    fn effective_page_size_defaults_when_zero() {
        assert_eq!(effective_page_size(0), DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn effective_page_size_clamps_to_max() {
        assert_eq!(effective_page_size(MAX_PAGE_SIZE + 1), MAX_PAGE_SIZE);
    }

    #[test]
    fn effective_page_size_passes_through_in_range() {
        assert_eq!(effective_page_size(17), 17);
    }

    fn sample_row(
        kind: crate::db::events::EventKind,
        payload: serde_json::Value,
    ) -> crate::db::events::EventRow {
        crate::db::events::EventRow {
            sequence: 1,
            kind: kind.as_str().to_owned(),
            payload,
            created_at: DateTime::<Utc>::from_timestamp(1_700_000_000, 0)
                .unwrap_or_else(|| panic!("valid unix timestamp")),
        }
    }

    #[test]
    fn replay_events_skips_attempt_created_rows() {
        let rows = [
            sample_row(
                crate::db::events::EventKind::AttemptCreated,
                serde_json::json!({ "attempt_number": 1 }),
            ),
            sample_row(
                crate::db::events::EventKind::StateChanged,
                serde_json::json!({
                    "state": "running",
                    "attempt_count": 1,
                    "failure": null,
                }),
            ),
        ];
        let wire = replay_events(&rows);
        assert_eq!(wire.len(), 1);
        assert!(matches!(
            wire[0].event,
            Some(generation_event::Event::StateChanged(_))
        ));
    }

    #[test]
    fn replay_events_stamps_emitted_at_from_the_persisted_row_not_now() {
        let rows = [sample_row(
            crate::db::events::EventKind::Progress,
            serde_json::json!({
                "fraction": 0.25,
                "stage": "denoise",
                "step": 1,
                "total_steps": 4,
            }),
        )];
        let wire = replay_events(&rows);
        let [event] = &wire[..] else {
            panic!("expected exactly one replayed event");
        };
        assert_eq!(
            event.emitted_at,
            crate::native::timestamp_to_proto(rows[0].created_at)
        );
    }

    #[test]
    fn replay_events_drops_a_malformed_payload() {
        let rows = [sample_row(
            crate::db::events::EventKind::Progress,
            serde_json::json!({ "unexpected": true }),
        )];
        assert!(replay_events(&rows).is_empty());
    }

    #[test]
    fn discontinuity_event_reports_the_given_reason() {
        let wire = discontinuity_event("missed live events while reconnecting");
        let Some(generation_event::Event::Discontinuity(discontinuity)) = wire.event else {
            panic!("expected a Discontinuity event");
        };
        assert_eq!(
            discontinuity.reason,
            "missed live events while reconnecting"
        );
    }
}
