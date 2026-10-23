//! Worker-local Artifact transfer (ADR 0008).
//!
//! Separate from the control Session so bulk bytes never block heartbeats or
//! cancellation (ADR 0004). Transfers move in bounded one-MiB chunks with a
//! declared size and SHA-256 digest validated before acceptance; a mismatch
//! is rejected without marking the Artifact available.

use buffa::EnumValue;
use connectrpc::{ConnectError, Encodable, ErrorCode, Response, ServiceResult, ServiceStream};
use futures::StreamExt;
use gpq_domain::{
    ArtifactId, ArtifactManifest, ArtifactState, AttemptId, ContentHash, Hasher, MediaKind,
    TenantId,
};
use gpq_proto::gpq::v1::MediaKind as ProtoMediaKind;
use gpq_proto::gpq::worker::v1::__buffa::oneof::deliver_artifact_request;
use gpq_proto::gpq::worker::v1::{
    ArtifactChunk, DeliverArtifactRequest, DeliverArtifactResponse, DeliverArtifactStart,
    FetchArtifactRequest, WorkerTransferService,
};

use crate::state::AppState;

/// Implements [`WorkerTransferService`] against shared Remote state.
#[derive(Clone)]
pub struct TransferApi {
    state: AppState,
}

impl TransferApi {
    /// Builds the service over `state`.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

/// Wraps any error as an internal Connect error. Generic over `Display`
/// rather than a concrete error type so it drops straight into `.map_err`
/// for both `sqlx::Result` and `anyhow::Result` call sites without an
/// intermediate closure.
fn internal(err: impl std::fmt::Display) -> ConnectError {
    ConnectError::new(ErrorCode::Internal, err.to_string())
}

fn unauthenticated() -> ConnectError {
    ConnectError::new(ErrorCode::Unauthenticated, "invalid worker credential")
}

fn parse_artifact_id(text: &str) -> Result<ArtifactId, ConnectError> {
    text.parse::<uuid::Uuid>()
        .map(ArtifactId::from_uuid)
        .map_err(|_| ConnectError::new(ErrorCode::InvalidArgument, "malformed artifact_id"))
}

fn parse_attempt_id(text: &str) -> Result<AttemptId, ConnectError> {
    text.parse::<uuid::Uuid>()
        .map(AttemptId::from_uuid)
        .map_err(|_| ConnectError::new(ErrorCode::InvalidArgument, "malformed attempt_id"))
}

/// Converts the wire manifest to the domain manifest, rejecting a malformed
/// digest or an unset/unknown media kind.
fn domain_manifest(
    proto: &gpq_proto::gpq::v1::ArtifactManifest,
) -> Result<ArtifactManifest, ConnectError> {
    let digest = proto.digest_sha256.parse::<ContentHash>().map_err(|err| {
        ConnectError::new(ErrorCode::InvalidArgument, format!("invalid digest: {err}"))
    })?;
    let kind = match proto.kind {
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_IMAGE) => MediaKind::Image,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_VIDEO) => MediaKind::Video,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_AUDIO) => MediaKind::Audio,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_TEXT) => MediaKind::Text,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_BINARY) => MediaKind::Binary,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_UNSPECIFIED) | EnumValue::Unknown(_) => {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "missing or unknown artifact media kind",
            ));
        }
    };
    Ok(ArtifactManifest {
        size_bytes: proto.size_bytes,
        digest,
        kind,
        mime_type: proto.mime_type.clone(),
    })
}

/// Plans the next transfer chunk: how many bytes it carries from `offset`,
/// bounded by ADR 0008's one-MiB chunks, and whether it ends the transfer.
///
/// Pure so the chunking rule is unit-testable without a database. A resume at
/// or past the end yields an empty final chunk, so the stream always
/// terminates cleanly.
fn next_chunk_span(total: u64, offset: u64) -> (u64, bool) {
    let remaining = total.saturating_sub(offset);
    let len = remaining.min(gpq_domain::TRANSFER_CHUNK_BYTES as u64);
    (len, len == remaining)
}

/// Streams `[start, total)` of a `remote_local` input one bounded chunk at a
/// time. Each chunk is a separate tenant-scoped read, so the whole Artifact is
/// never held in memory: the single buffer read for a chunk is moved straight
/// into its outgoing message.
fn stream_remote_local(
    state: AppState,
    tenant: TenantId,
    attempt: AttemptId,
    worker: gpq_domain::WorkerId,
    artifact: ArtifactId,
    start: u64,
    total: u64,
) -> impl futures::Stream<Item = Result<ArtifactChunk, ConnectError>> + Send + 'static {
    futures::stream::try_unfold(Some(start), move |cursor| {
        let state = state.clone();
        async move {
            let Some(offset) = cursor else {
                return Ok(None);
            };
            let (len, last) = next_chunk_span(total, offset);
            let mut conn = state.db.begin_tenant(tenant).await.map_err(internal)?;
            let data = crate::db::artifacts::remote_local_chunk(
                &mut conn, tenant, attempt, worker, artifact, offset, len,
            )
            .await
            .map_err(internal)?;
            conn.commit().await.map_err(internal)?;
            let Some(data) = data else {
                return Err(ConnectError::new(
                    ErrorCode::FailedPrecondition,
                    "artifact is no longer downloadable",
                ));
            };
            if u64::try_from(data.len()).ok() != Some(len) {
                return Err(internal("stored artifact is shorter than its manifest"));
            }
            let next = (!last).then_some(offset + len);
            Ok(Some((
                ArtifactChunk {
                    offset,
                    data,
                    last,
                    ..Default::default()
                },
                next,
            )))
        }
    })
}

impl WorkerTransferService for TransferApi {
    async fn fetch_artifact(
        &self,
        ctx: connectrpc::RequestContext,
        request: connectrpc::ServiceRequest<'_, FetchArtifactRequest>,
    ) -> ServiceResult<ServiceStream<impl Encodable<ArtifactChunk> + Send + use<>>> {
        let Some(token) = crate::auth::bearer_token(ctx.headers()) else {
            return Err(unauthenticated());
        };
        let Some((tenant_id, worker_id)) = self
            .state
            .db
            .authenticate_worker(token)
            .await
            .map_err(internal)?
        else {
            return Err(unauthenticated());
        };

        let request = request.to_owned_message();
        let artifact_id = parse_artifact_id(&request.artifact_id)?;
        let attempt_id = parse_attempt_id(&request.attempt_id)?;

        // Only the Worker holding a live lease on the Attempt may read its
        // inputs, and only inputs of that Attempt's Generation (ADR 0008).
        // Serving does not consume the input: it stays available until the
        // Generation terminates.
        let mut tx = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let row = crate::db::artifacts::attempt_input(
            &mut tx,
            tenant_id,
            attempt_id,
            worker_id,
            artifact_id,
        )
        .await
        .map_err(internal)?;
        let Some(row) = row else {
            return Err(ConnectError::new(ErrorCode::NotFound, "artifact not found"));
        };
        if row.placement != gpq_domain::ArtifactPlacement::RemoteLocal
            || row.state != ArtifactState::Available
        {
            return Err(ConnectError::new(
                ErrorCode::FailedPrecondition,
                "artifact is not available for direct transfer",
            ));
        }
        let total = row.manifest.size_bytes;
        tx.commit().await.map_err(internal)?;
        if request.offset > total {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "offset is past the end of the artifact",
            ));
        }
        Response::stream_ok(stream_remote_local(
            self.state.clone(),
            tenant_id,
            attempt_id,
            worker_id,
            artifact_id,
            request.offset,
            total,
        ))
    }

    #[expect(
        refining_impl_trait,
        reason = "gpq-remote is a bin-only crate with no library surface; this RPITIT is never part of a published API needing exact auto-trait parity with the macro-generated trait declaration"
    )]
    async fn deliver_artifact(
        &self,
        ctx: connectrpc::RequestContext,
        requests: connectrpc::InboundStream<DeliverArtifactRequest>,
    ) -> ServiceResult<DeliverArtifactResponse> {
        let Some(token) = crate::auth::bearer_token(ctx.headers()) else {
            return Err(unauthenticated());
        };
        let Some((tenant_id, _worker_id)) = self
            .state
            .db
            .authenticate_worker(token)
            .await
            .map_err(internal)?
        else {
            return Err(unauthenticated());
        };

        let mut requests = requests;
        let Some(first) = requests.next().await else {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "empty delivery stream",
            ));
        };
        let first = first?.to_owned_message();
        let Some(deliver_artifact_request::Message::Start(start)) = first.message else {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "first message must be DeliverArtifactStart",
            ));
        };

        let start_offset = start.offset;
        let (artifact_id, manifest) =
            validate_delivery_start(&self.state, tenant_id, *start).await?;
        let receipt = receive_delivery_chunks(
            &self.state,
            artifact_id,
            &manifest,
            start_offset,
            &mut requests,
        )
        .await?;
        let (committed_offset, digest) = match receipt {
            ChunkReceipt::NoDownloader { committed_offset } => {
                return Response::ok(DeliverArtifactResponse {
                    accepted: false,
                    reason: "no active download".to_owned(),
                    committed_offset,
                    ..Default::default()
                });
            }
            ChunkReceipt::Delivered {
                committed_offset,
                digest,
            } => (committed_offset, digest),
        };

        let failure =
            match validate_delivery_receipt(&manifest, start_offset, committed_offset, digest) {
                DeliveryValidation::Accepted => None,
                DeliveryValidation::Retryable(reason) => {
                    Some(crate::artifacts::DeliveryChunk::Failed(reason))
                }
                DeliveryValidation::Rejected(reason) => {
                    Some(crate::artifacts::DeliveryChunk::Rejected(reason))
                }
            };
        if let Some(failure) = failure {
            let reason = match &failure {
                crate::artifacts::DeliveryChunk::Failed(reason)
                | crate::artifacts::DeliveryChunk::Rejected(reason) => reason.clone(),
                crate::artifacts::DeliveryChunk::Data(_)
                | crate::artifacts::DeliveryChunk::Complete => unreachable!(),
            };
            self.state
                .artifacts
                .push_delivery(artifact_id, failure)
                .await;
            return Response::ok(DeliverArtifactResponse {
                accepted: false,
                reason,
                committed_offset,
                ..Default::default()
            });
        }

        self.state
            .artifacts
            .push_delivery(artifact_id, crate::artifacts::DeliveryChunk::Complete)
            .await;

        Response::ok(DeliverArtifactResponse {
            accepted: true,
            reason: String::new(),
            committed_offset,
            ..Default::default()
        })
    }
}

/// Validates a `DeliverArtifactStart` against the recorded output Artifact:
/// it must exist, be `delivering`, match the declared manifest, and present
/// the matching delivery token. Extracted from
/// [`TransferApi::deliver_artifact`] to keep that method under clippy's
/// line-count ceiling.
async fn validate_delivery_start(
    state: &AppState,
    tenant_id: TenantId,
    start: DeliverArtifactStart,
) -> Result<(ArtifactId, ArtifactManifest), ConnectError> {
    let artifact_id = parse_artifact_id(&start.artifact_id)?;
    let Some(manifest_proto) = start.manifest.into_option() else {
        return Err(ConnectError::new(
            ErrorCode::InvalidArgument,
            "missing artifact manifest",
        ));
    };
    let manifest = domain_manifest(&manifest_proto)?;

    let mut tx = state.db.begin_tenant(tenant_id).await.map_err(internal)?;
    let row = crate::db::artifacts::get(&mut tx, tenant_id, artifact_id)
        .await
        .map_err(internal)?;
    tx.commit().await.map_err(internal)?;

    let Some(row) = row else {
        return Err(ConnectError::new(ErrorCode::NotFound, "artifact not found"));
    };
    if row.state != ArtifactState::Delivering {
        return Err(ConnectError::new(
            ErrorCode::FailedPrecondition,
            "artifact is not currently being delivered",
        ));
    }
    if row.manifest != manifest {
        return Err(ConnectError::new(
            ErrorCode::FailedPrecondition,
            "delivered manifest does not match the recorded output manifest",
        ));
    }
    if start.offset > row.manifest.size_bytes {
        return Err(ConnectError::new(
            ErrorCode::FailedPrecondition,
            "delivery offset is past the artifact size",
        ));
    }
    if row.committed_offset != start.offset {
        return Err(ConnectError::new(
            ErrorCode::FailedPrecondition,
            "delivery offset does not match the recorded progress",
        ));
    }
    match &row.delivery_token {
        Some(expected) if *expected == start.delivery_token => {}
        _ => {
            return Err(ConnectError::new(
                ErrorCode::FailedPrecondition,
                "delivery token mismatch",
            ));
        }
    }
    Ok((artifact_id, manifest))
}

/// Whether a received delivery can complete, should be retried, or must be
/// rejected permanently. A resumed RPC can validate only its suffix length;
/// the HTTP stream separately hashes all chunks across internal resumptions
/// before it marks the Artifact consumed.
enum DeliveryValidation {
    Accepted,
    Retryable(String),
    Rejected(String),
}

fn validate_delivery_receipt(
    manifest: &ArtifactManifest,
    start_offset: u64,
    committed_offset: u64,
    digest: ContentHash,
) -> DeliveryValidation {
    if start_offset == 0 {
        return match manifest.verify(committed_offset, digest) {
            Ok(()) => DeliveryValidation::Accepted,
            Err(mismatch @ gpq_domain::ManifestMismatch::Digest { .. }) => {
                DeliveryValidation::Rejected(mismatch.to_string())
            }
            Err(mismatch) => DeliveryValidation::Retryable(mismatch.to_string()),
        };
    }
    if committed_offset != manifest.size_bytes {
        return DeliveryValidation::Retryable(format!(
            "resumed delivery ended at {committed_offset} bytes, expected {}",
            manifest.size_bytes
        ));
    }
    DeliveryValidation::Accepted
}

/// Outcome of receiving a Worker's `DeliverArtifact` chunk stream through to
/// completion or a lost downloader (ADR 0008).
enum ChunkReceipt {
    /// Every chunk reached the subscribed downloader; digest ready to verify.
    Delivered {
        committed_offset: u64,
        digest: ContentHash,
    },
    /// No downloader was subscribed to receive the bytes.
    NoDownloader { committed_offset: u64 },
}

/// Returns the offset immediately after a chunk, rejecting gaps, overlaps,
/// and integer overflow in the worker-supplied chunk stream.
fn next_delivery_offset(expected_offset: u64, chunk: &ArtifactChunk) -> Result<u64, String> {
    if chunk.offset != expected_offset {
        return Err(format!(
            "artifact chunk offset {} does not match expected offset {}",
            chunk.offset, expected_offset
        ));
    }
    expected_offset
        .checked_add(
            u64::try_from(chunk.data.len())
                .map_err(|_| "artifact chunk length cannot be represented as a u64".to_owned())?,
        )
        .ok_or_else(|| "artifact chunk offset overflows u64".to_owned())
}

/// Reads a Worker's `DeliverArtifact` chunk stream, relaying each chunk to
/// the subscribed downloader (if any) and hashing it. Extracted from
/// [`TransferApi::deliver_artifact`] to keep that method under clippy's
/// line-count ceiling. A stream that ends before its final chunk is a
/// protocol violation, rejected as an error rather than a receipt.
async fn receive_delivery_chunks(
    state: &AppState,
    artifact_id: ArtifactId,
    manifest: &ArtifactManifest,
    start_offset: u64,
    requests: &mut connectrpc::InboundStream<DeliverArtifactRequest>,
) -> Result<ChunkReceipt, ConnectError> {
    let mut hasher = Hasher::new();
    let mut next_offset = start_offset;
    let mut saw_last = false;

    while let Some(item) = requests.next().await {
        let item = item?.to_owned_message();
        let Some(deliver_artifact_request::Message::Chunk(chunk)) = item.message else {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "expected an ArtifactChunk after DeliverArtifactStart",
            ));
        };
        let following_offset = match next_delivery_offset(next_offset, &chunk) {
            Ok(offset)
                if chunk.data.len() <= gpq_domain::TRANSFER_CHUNK_BYTES
                    && offset <= manifest.size_bytes =>
            {
                offset
            }
            Ok(_) => {
                let reason = "artifact chunk exceeds its transfer or manifest size limit";
                state
                    .artifacts
                    .push_delivery(
                        artifact_id,
                        crate::artifacts::DeliveryChunk::Rejected(reason.to_owned()),
                    )
                    .await;
                return Err(ConnectError::new(ErrorCode::InvalidArgument, reason));
            }
            Err(reason) => {
                state
                    .artifacts
                    .push_delivery(
                        artifact_id,
                        crate::artifacts::DeliveryChunk::Rejected(reason.clone()),
                    )
                    .await;
                return Err(ConnectError::new(ErrorCode::InvalidArgument, reason));
            }
        };
        hasher.update(&chunk.data);
        let is_last = chunk.last;
        next_offset = following_offset;

        let downloader_attached = state
            .artifacts
            .push_delivery(
                artifact_id,
                crate::artifacts::DeliveryChunk::Data(chunk.data),
            )
            .await;
        if !downloader_attached {
            return Ok(ChunkReceipt::NoDownloader {
                committed_offset: following_offset,
            });
        }
        if is_last {
            saw_last = true;
            break;
        }
    }
    if !saw_last {
        state
            .artifacts
            .push_delivery(
                artifact_id,
                crate::artifacts::DeliveryChunk::Failed(
                    "delivery stream ended before the final chunk".to_owned(),
                ),
            )
            .await;
        return Err(ConnectError::new(
            ErrorCode::InvalidArgument,
            "delivery stream ended before the final chunk",
        ));
    }
    Ok(ChunkReceipt::Delivered {
        committed_offset: next_offset,
        digest: hasher.finish(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_cover_the_whole_artifact_in_bounded_spans() {
        let chunk = gpq_domain::TRANSFER_CHUNK_BYTES as u64;
        assert_eq!(next_chunk_span(chunk + 10, 0), (chunk, false));
        assert_eq!(next_chunk_span(chunk + 10, chunk), (10, true));
    }

    #[test]
    fn resume_offset_skips_already_delivered_bytes() {
        assert_eq!(next_chunk_span(100, 40), (60, true));
    }

    #[test]
    fn a_single_chunk_fitting_exactly_is_marked_last() {
        assert_eq!(next_chunk_span(5, 0), (5, true));
    }

    #[test]
    fn resume_past_the_end_yields_one_empty_last_chunk() {
        assert_eq!(next_chunk_span(5, 100), (0, true));
    }

    #[test]
    fn empty_artifact_yields_one_empty_last_chunk() {
        assert_eq!(next_chunk_span(0, 0), (0, true));
    }

    #[test]
    fn delivery_chunks_must_start_at_the_recorded_offset() {
        let chunk = ArtifactChunk {
            offset: 10,
            data: vec![1, 2, 3],
            ..Default::default()
        };

        assert_eq!(next_delivery_offset(10, &chunk), Ok(13));
        assert!(next_delivery_offset(9, &chunk).is_err());
        assert!(next_delivery_offset(13, &chunk).is_err());
    }

    #[test]
    fn delivery_chunk_offset_overflow_is_rejected() {
        let chunk = ArtifactChunk {
            offset: u64::MAX,
            data: vec![1],
            ..Default::default()
        };

        assert!(next_delivery_offset(u64::MAX, &chunk).is_err());
    }

    #[test]
    fn initial_digest_mismatch_is_permanently_rejected() {
        let manifest = ArtifactManifest {
            size_bytes: 4,
            digest: ContentHash::digest(b"good"),
            kind: MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        };

        assert!(matches!(
            validate_delivery_receipt(&manifest, 0, 4, ContentHash::digest(b"bad!")),
            DeliveryValidation::Rejected(_)
        ));
    }

    #[test]
    fn incomplete_delivery_remains_retryable() {
        let manifest = ArtifactManifest {
            size_bytes: 4,
            digest: ContentHash::digest(b"good"),
            kind: MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        };

        assert!(matches!(
            validate_delivery_receipt(&manifest, 0, 3, ContentHash::digest(b"bad")),
            DeliveryValidation::Retryable(_)
        ));
        assert!(matches!(
            validate_delivery_receipt(&manifest, 3, 3, ContentHash::digest(b"bad")),
            DeliveryValidation::Retryable(_)
        ));
    }

    #[test]
    fn manifest_conversion_rejects_malformed_digest() {
        let proto = gpq_proto::gpq::v1::ArtifactManifest {
            size_bytes: 4,
            digest_sha256: "not-hex".to_owned(),
            kind: EnumValue::Known(ProtoMediaKind::MEDIA_KIND_BINARY),
            mime_type: "application/octet-stream".to_owned(),
            ..Default::default()
        };
        assert!(domain_manifest(&proto).is_err());
    }

    #[test]
    fn manifest_conversion_rejects_unknown_media_kind() {
        let hash = ContentHash::from_bytes([9; 32]);
        let proto = gpq_proto::gpq::v1::ArtifactManifest {
            size_bytes: 4,
            digest_sha256: hash.to_hex(),
            kind: EnumValue::Known(ProtoMediaKind::MEDIA_KIND_UNSPECIFIED),
            mime_type: "application/octet-stream".to_owned(),
            ..Default::default()
        };
        assert!(domain_manifest(&proto).is_err());
    }

    #[test]
    fn manifest_conversion_accepts_a_well_formed_manifest() {
        let hash = ContentHash::from_bytes([3; 32]);
        let proto = gpq_proto::gpq::v1::ArtifactManifest {
            size_bytes: 4,
            digest_sha256: hash.to_hex(),
            kind: EnumValue::Known(ProtoMediaKind::MEDIA_KIND_IMAGE),
            mime_type: "image/png".to_owned(),
            ..Default::default()
        };
        let Ok(manifest) = domain_manifest(&proto) else {
            panic!("expected a valid manifest");
        };
        assert_eq!(manifest.digest, hash);
        assert_eq!(manifest.kind, MediaKind::Image);
        assert_eq!(manifest.mime_type, "image/png");
    }

    #[test]
    fn an_artifact_exactly_a_multiple_of_the_chunk_size_ends_on_a_boundary() {
        // ADR 0008: chunk offsets/last-flag at exact multiples of
        // TRANSFER_CHUNK_BYTES must not produce a trailing empty chunk.
        let total = gpq_domain::TRANSFER_CHUNK_BYTES as u64 * 2;
        let chunk = gpq_domain::TRANSFER_CHUNK_BYTES as u64;
        assert_eq!(next_chunk_span(total, 0), (chunk, false));
        assert_eq!(next_chunk_span(total, chunk), (chunk, true));
    }

    #[test]
    fn delivered_manifest_rejects_a_size_mismatch() {
        // ADR 0008: a size mismatch is rejected without marking the
        // Artifact available — the same `verify` call `deliver_artifact`
        // gates acceptance on.
        let manifest = ArtifactManifest {
            size_bytes: 4,
            digest: ContentHash::digest(b"abcd"),
            kind: MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        };
        assert_eq!(
            manifest.verify(3, ContentHash::digest(b"abc")),
            Err(gpq_domain::ManifestMismatch::Size {
                declared: 4,
                received: 3
            })
        );
    }

    #[test]
    fn delivered_manifest_rejects_a_digest_mismatch() {
        let manifest = ArtifactManifest {
            size_bytes: 4,
            digest: ContentHash::digest(b"abcd"),
            kind: MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        };
        assert!(matches!(
            manifest.verify(4, ContentHash::digest(b"wxyz")),
            Err(gpq_domain::ManifestMismatch::Digest { .. })
        ));
    }
}
