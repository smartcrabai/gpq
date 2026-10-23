//! A protocol-level stand-in for a Worker (ADR 0004): drives
//! `WorkerEnrollmentService`, `WorkerSessionService`, and
//! `WorkerTransferService` directly with the generated `connectrpc` clients.
//!
//! The real `gpq worker` cannot execute Generations on a CI host (its
//! inference child needs an accelerator and model weights), so every test that
//! must lease, run, fail, or fetch input for a Generation drives one of these.
//! It is also the only way to produce what a real Worker never does: a
//! handshake with a foreign protocol major, and late or stale results racing
//! cancellation or lease expiry (ADR 0003).
//!
//! Each synthetic Worker advertises one ready Pool resident on a Model Version
//! whose asset hashes derive from its own name, so Remote's scheduler never
//! routes one test's Generation to another test's Worker.

use std::time::Duration;

use anyhow::{Context, bail};
use connectrpc::client::{CallOptions, ClientConfig, Http2Connection, SharedHttp2Connection};
use connectrpc::{ConnectError, Protocol};
use gpq_domain::{ContentHash, ModelAssetRole, ModelFamily, ModelManifest};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use gpq_proto::gpq::worker::v1::__buffa::oneof::remote_message;
use rand::Rng;

use super::{Harness, wait_until};

/// The bidirectional control stream a synthetic Worker holds for as long as
/// it stays connected, typed for the plaintext HTTP/2 transport every harness
/// `gpq remote serve` speaks.
type SyntheticStream = connectrpc::client::BidiStream<
    hyper::body::Incoming,
    wpb::WorkerMessage,
    <wpb::RemoteMessage as buffa::HasMessageView>::View<'static>,
>;

/// The Model Version a synthetic Worker advertises for `name` and `family`.
/// Asset hashes are derived from `name`, so every test's version is distinct
/// and the scheduler never routes work across tests.
#[must_use]
pub fn manifest_for(name: &str, family: ModelFamily) -> ModelManifest {
    let roles: &[ModelAssetRole] = match family {
        ModelFamily::QwenImage21 => &[
            ModelAssetRole::Diffusion,
            ModelAssetRole::TextEncoder,
            ModelAssetRole::VisionEncoder,
            ModelAssetRole::Vae,
        ],
        ModelFamily::MinimaxH3 => &[
            ModelAssetRole::Diffusion,
            ModelAssetRole::TextEncoder,
            ModelAssetRole::Vae,
            ModelAssetRole::AudioVae,
        ],
    };
    ModelManifest {
        family,
        assets: roles
            .iter()
            .map(|role| {
                (
                    *role,
                    ContentHash::digest(format!("{name}:{role:?}").as_bytes()),
                )
            })
            .collect(),
        default_loras: Vec::new(),
        sampling: gpq_domain::SamplingPreset {
            steps: 4,
            cfg_scale: 1.0,
            sigmas: Vec::new(),
        },
    }
}

fn proto_family(family: ModelFamily) -> pb::ModelFamily {
    match family {
        ModelFamily::QwenImage21 => pb::ModelFamily::MODEL_FAMILY_QWEN_IMAGE21,
        ModelFamily::MinimaxH3 => pb::ModelFamily::MODEL_FAMILY_MINIMAX_H3,
    }
}

fn proto_role(role: ModelAssetRole) -> pb::ModelAssetRole {
    match role {
        ModelAssetRole::Diffusion => pb::ModelAssetRole::MODEL_ASSET_ROLE_DIFFUSION,
        ModelAssetRole::TextEncoder => pb::ModelAssetRole::MODEL_ASSET_ROLE_TEXT_ENCODER,
        ModelAssetRole::VisionEncoder => pb::ModelAssetRole::MODEL_ASSET_ROLE_VISION_ENCODER,
        ModelAssetRole::Vae => pb::ModelAssetRole::MODEL_ASSET_ROLE_VAE,
        ModelAssetRole::AudioVae => pb::ModelAssetRole::MODEL_ASSET_ROLE_AUDIO_VAE,
    }
}

/// Converts a domain `ModelManifest` to its wire form, the shape Remote
/// receives in a capability report and re-derives the content hash from.
fn proto_manifest(manifest: &ModelManifest) -> pb::ModelManifest {
    pb::ModelManifest {
        family: proto_family(manifest.family).into(),
        assets: manifest
            .assets
            .iter()
            .map(|(role, hash)| pb::ModelAsset {
                role: proto_role(*role).into(),
                sha256: hash.to_hex(),
                ..Default::default()
            })
            .collect(),
        default_loras: manifest
            .default_loras
            .iter()
            .map(|lora| pb::LoraSelection {
                sha256: lora.sha256.to_hex(),
                scale: lora.scale,
                ..Default::default()
            })
            .collect(),
        sampling: pb::SamplingPreset {
            steps: manifest.sampling.steps,
            cfg_scale: manifest.sampling.cfg_scale,
            sigmas: manifest.sampling.sigmas.clone(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

/// One ready, unbusy Pool serving exactly `manifest`.
fn capability_report(manifest: &ModelManifest, version_sha256: &str) -> wpb::CapabilityReport {
    wpb::CapabilityReport {
        pools: vec![wpb::PoolAdvertisement {
            pool_id: "synthetic-pool".to_owned(),
            accelerator: "synthetic".to_owned(),
            backend_version: "synthetic".to_owned(),
            ready: true,
            slots: vec![wpb::SlotAdvertisement {
                slot_id: "slot0".to_owned(),
                busy: false,
                attempt_id: String::new(),
                ..Default::default()
            }],
            resident_model_sha256: version_sha256.to_owned(),
            models: vec![pb::ModelAdvertisement {
                version_sha256: version_sha256.to_owned(),
                manifest: proto_manifest(manifest).into(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn shared_transport(remote_uri: &http::Uri) -> anyhow::Result<SharedHttp2Connection> {
    let connection = Http2Connection::connect_plaintext(remote_uri.clone())
        .await
        .context("connecting to remote for a synthetic worker")?;
    Ok(connection.shared(16))
}

/// Enrolls `worker_name` under the Tenant that owns `master_key`, returning
/// the worker id and Worker Credential (ADR 0009).
///
/// # Errors
/// Returns an error if the connection or the Enroll RPC fails.
pub async fn enroll(
    remote_uri: &http::Uri,
    master_key: &str,
    worker_name: &str,
) -> anyhow::Result<(String, String)> {
    let transport = shared_transport(remote_uri).await?;
    let config = ClientConfig::new(remote_uri.clone())
        .with_protocol(Protocol::Grpc)
        .with_default_header("authorization", format!("Bearer {master_key}"));
    let client = wpb::WorkerEnrollmentServiceClient::new(transport, config);
    let request = wpb::EnrollRequest {
        worker_name: worker_name.to_owned(),
        host_descriptor: "gpq-remote-e2e-harness".to_owned(),
        protocol_major: gpq_proto::PROTOCOL_MAJOR,
        protocol_minor: gpq_proto::PROTOCOL_MINOR,
        worker_version: "e2e-test".to_owned(),
        ..Default::default()
    };
    let response = client
        .enroll(request)
        .await
        .map_err(|err| anyhow::anyhow!("Enroll RPC failed: {err}"))?
        .into_owned();
    Ok((response.worker_id, response.worker_credential))
}

/// Opens a control Session and sends a `Handshake` reporting `protocol_major`,
/// returning the stream and `HandshakeAck`, or the `ConnectError` Remote
/// rejected the handshake with (ADR 0004).
async fn open_session(
    remote_uri: &http::Uri,
    credential: &str,
    protocol_major: u32,
) -> anyhow::Result<Result<(SyntheticStream, wpb::HandshakeAck), ConnectError>> {
    let transport = shared_transport(remote_uri).await?;
    let config = ClientConfig::new(remote_uri.clone())
        .with_protocol(Protocol::Grpc)
        .with_default_header("authorization", format!("Bearer {credential}"));
    let client = wpb::WorkerSessionServiceClient::new(transport, config);
    let mut stream = client
        .session()
        .await
        .context("opening the synthetic worker's control session")?;
    stream
        .send(wpb::WorkerMessage {
            message: Some(
                wpb::Handshake {
                    protocol_major,
                    protocol_minor: gpq_proto::PROTOCOL_MINOR,
                    worker_version: "e2e-synthetic".to_owned(),
                    host_descriptor: "gpq-remote-e2e-synthetic".to_owned(),
                    ..Default::default()
                }
                .into(),
            ),
            ..Default::default()
        })
        .await
        .context("sending the synthetic worker's Handshake")?;

    match stream.message::<wpb::RemoteMessage>().await {
        Ok(Some(message)) => {
            let owned = message.to_owned_message();
            let Some(remote_message::Message::HandshakeAck(ack)) = owned.message else {
                bail!("expected a HandshakeAck as the synthetic worker's first message");
            };
            Ok(Ok((stream, *ack)))
        }
        Ok(None) => bail!("remote closed the session before handshaking"),
        Err(err) => Ok(Err(err)),
    }
}

/// A Worker impersonated at the protocol layer over its control Session, with
/// one Pool serving one Model Version and one Model alias for that version.
pub struct SyntheticWorker {
    /// The Worker's enrolled name.
    pub name: String,
    /// The Model alias that resolves to this Worker's Model Version.
    pub alias: String,
    /// The content hash of the Model Version this Worker advertises.
    pub version_sha256: String,
    /// The enrolled Worker id, for administration commands.
    pub worker_id: String,
    credential: String,
    remote_uri: http::Uri,
    manifest: ModelManifest,
    stream: Option<SyntheticStream>,
}

impl SyntheticWorker {
    /// Enrolls `name`, connects, advertises a ready Pool serving a Model
    /// Version for `family`, waits for Remote to register it, and aliases it
    /// so `alias` schedules exclusively onto this Worker.
    ///
    /// # Errors
    /// Returns an error if enrollment, the handshake, registration, or
    /// aliasing fails.
    pub async fn spawn(harness: &Harness, name: &str, family: ModelFamily) -> anyhow::Result<Self> {
        let remote_uri: http::Uri = harness
            .url("")
            .parse()
            .context("parsing the remote base uri")?;
        let manifest = manifest_for(name, family);
        let version_sha256 = manifest
            .canonical_hash()
            .context("hashing the synthetic model manifest")?
            .to_hex();
        let (worker_id, credential) =
            enroll(&remote_uri, &harness.tenant1.master_key, name).await?;
        let mut worker = Self {
            name: name.to_owned(),
            alias: format!("{name}-alias"),
            version_sha256,
            worker_id,
            credential,
            remote_uri,
            manifest,
            stream: None,
        };
        worker.connect().await?;
        worker.wait_for_registration(harness).await?;
        harness
            .catalog_client(&harness.tenant1.master_key)
            .set_model_alias(pb::SetModelAliasRequest {
                alias: worker.alias.clone(),
                content_sha256: worker.version_sha256.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| {
                anyhow::anyhow!("SetModelAlias for the synthetic worker failed: {err}")
            })?;
        Ok(worker)
    }

    /// Enrolls `name` and attempts the handshake with a deliberately wrong
    /// `protocol_major`, returning the `ConnectError` Remote rejects it with
    /// (ADR 0004).
    ///
    /// # Errors
    /// Returns an error if enrollment fails or the handshake unexpectedly
    /// succeeds.
    pub async fn spawn_with_protocol_major(
        harness: &Harness,
        name: &str,
        protocol_major: u32,
    ) -> anyhow::Result<ConnectError> {
        let remote_uri: http::Uri = harness
            .url("")
            .parse()
            .context("parsing the remote base uri")?;
        let (_worker_id, credential) =
            enroll(&remote_uri, &harness.tenant1.master_key, name).await?;
        match open_session(&remote_uri, &credential, protocol_major).await? {
            Ok(_) => bail!(
                "synthetic worker handshake unexpectedly succeeded with protocol_major {protocol_major}"
            ),
            Err(err) => Ok(err),
        }
    }

    /// Enrolls `name` under the Tenant that owns `master_key`, without opening
    /// a control Session. Used to present a Worker Credential from another
    /// Tenant to Worker-only RPCs (ADR 0009, ADR 0011).
    ///
    /// # Errors
    /// Returns an error if enrollment fails.
    pub async fn enrolled_in(
        harness: &Harness,
        master_key: &str,
        name: &str,
    ) -> anyhow::Result<Self> {
        let remote_uri: http::Uri = harness
            .url("")
            .parse()
            .context("parsing the remote base uri")?;
        let (worker_id, credential) = enroll(&remote_uri, master_key, name).await?;
        Ok(Self {
            name: name.to_owned(),
            alias: String::new(),
            version_sha256: String::new(),
            worker_id,
            credential,
            remote_uri,
            manifest: manifest_for(name, ModelFamily::QwenImage21),
            stream: None,
        })
    }

    /// Re-opens the control Session with the same Worker Credential and
    /// re-advertises the same Pool, as a restarted Worker does (ADR 0003).
    ///
    /// # Errors
    /// Returns an error if the handshake is rejected or registration fails.
    pub async fn reconnect(&mut self, harness: &Harness) -> anyhow::Result<()> {
        self.connect().await?;
        self.wait_for_registration(harness).await
    }

    /// Drops the control Session, as a Worker crash or network loss does.
    /// Leased Attempts then lapse on their own (ADR 0003).
    pub fn disconnect(&mut self) {
        self.stream = None;
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        let (stream, _ack) = open_session(
            &self.remote_uri,
            &self.credential,
            gpq_proto::PROTOCOL_MAJOR,
        )
        .await?
        .map_err(|err| anyhow::anyhow!("synthetic worker handshake failed: {err}"))?;
        self.stream = Some(stream);
        let report = capability_report(&self.manifest, &self.version_sha256);
        self.send(report).await
    }

    async fn wait_for_registration(&self, harness: &Harness) -> anyhow::Result<()> {
        wait_until(
            || async {
                let response = harness
                    .catalog_client(&harness.tenant1.master_key)
                    .list_workers(pb::ListWorkersRequest::default())
                    .await
                    .map_err(|err| anyhow::anyhow!("ListWorkers failed: {err}"))?
                    .into_owned();
                let ready = response.workers.iter().any(|worker| {
                    worker.name == self.name
                        && worker.online
                        && worker.model_sha256.contains(&self.version_sha256)
                        && worker.pools.iter().any(|pool| pool.total_slots > 0)
                });
                Ok(ready.then_some(()))
            },
            Duration::from_secs(30),
        )
        .await
        .context("waiting for the synthetic worker's Pool to register")
    }

    /// Sends one `WorkerMessage` over the control Session.
    ///
    /// # Errors
    /// Returns an error if the Session is closed or the send fails.
    pub async fn send(
        &mut self,
        message: impl Into<wpb::__buffa::oneof::worker_message::Message>,
    ) -> anyhow::Result<()> {
        let stream = self
            .stream
            .as_mut()
            .context("the synthetic worker's control session is closed")?;
        stream
            .send(wpb::WorkerMessage {
                message: Some(message.into()),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("synthetic worker send failed: {err}"))
    }

    /// Reads `RemoteMessage`s until `matches` returns `Some`, discarding every
    /// message it rejects, or `timeout` elapses.
    async fn recv_until<T>(
        &mut self,
        timeout: Duration,
        mut matches: impl FnMut(wpb::RemoteMessage) -> Option<T>,
    ) -> anyhow::Result<T> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            anyhow::ensure!(
                remaining > Duration::ZERO,
                "timed out waiting for a matching RemoteMessage"
            );
            let stream = self
                .stream
                .as_mut()
                .context("the synthetic worker's control session is closed")?;
            let next = tokio::time::timeout(remaining, stream.message::<wpb::RemoteMessage>())
                .await
                .context("timed out waiting for a RemoteMessage")?
                .map_err(|err| anyhow::anyhow!("synthetic worker session stream error: {err}"))?
                .context("remote closed the synthetic worker's session")?
                .to_owned_message();
            if let Some(value) = matches(next) {
                return Ok(value);
            }
        }
    }

    /// Waits for the next `LeaseAssignment`.
    ///
    /// # Errors
    /// Returns an error if the session errors or `timeout` elapses first.
    pub async fn recv_lease(&mut self, timeout: Duration) -> anyhow::Result<wpb::LeaseAssignment> {
        self.recv_until(timeout, |message| match message.message {
            Some(remote_message::Message::Lease(lease)) => Some(*lease),
            _ => None,
        })
        .await
    }

    /// Waits for the next `CancelRequest`.
    ///
    /// # Errors
    /// Returns an error if the session errors or `timeout` elapses first.
    pub async fn recv_cancel_request(
        &mut self,
        timeout: Duration,
    ) -> anyhow::Result<wpb::CancelRequest> {
        self.recv_until(timeout, |message| match message.message {
            Some(remote_message::Message::Cancel(cancel)) => Some(*cancel),
            _ => None,
        })
        .await
    }

    /// Waits for the `DiscardOutput` naming `delivery_token`.
    ///
    /// # Errors
    /// Returns an error if the session errors or `timeout` elapses first.
    pub async fn recv_discard(
        &mut self,
        timeout: Duration,
        delivery_token: &str,
    ) -> anyhow::Result<wpb::DiscardOutput> {
        self.recv_until(timeout, |message| match message.message {
            Some(remote_message::Message::Discard(discard))
                if discard.delivery_token == delivery_token =>
            {
                Some(*discard)
            }
            _ => None,
        })
        .await
    }

    /// Acknowledges that `attempt_id` has stopped after a `CancelRequest`.
    ///
    /// # Errors
    /// Returns an error if the send fails.
    pub async fn acknowledge_cancel(&mut self, attempt_id: &str) -> anyhow::Result<()> {
        self.send(wpb::CancelAcknowledged {
            attempt_id: attempt_id.to_owned(),
            ..Default::default()
        })
        .await
    }

    /// Reports that `attempt_id` has started executing.
    ///
    /// # Errors
    /// Returns an error if the send fails.
    pub async fn start_attempt(&mut self, attempt_id: &str) -> anyhow::Result<()> {
        self.send(wpb::AttemptRunning {
            attempt_id: attempt_id.to_owned(),
            ..Default::default()
        })
        .await
    }

    /// Renews the leases of `attempt_ids` (ADR 0003).
    ///
    /// # Errors
    /// Returns an error if the send fails.
    pub async fn heartbeat(&mut self, attempt_ids: &[String]) -> anyhow::Result<()> {
        self.send(wpb::Heartbeat {
            attempt_ids: attempt_ids.to_vec(),
            ..Default::default()
        })
        .await
    }

    /// Commits one worker-local image or video output for `lease` as its
    /// result, returning the delivery token Remote will use to ask for it.
    /// Remote decides whether the result is accepted; this only sends it.
    ///
    /// # Errors
    /// Returns an error if the send fails.
    pub async fn complete_attempt(
        &mut self,
        attempt_id: &str,
        bytes: &[u8],
        kind: pb::MediaKind,
        mime_type: &str,
    ) -> anyhow::Result<String> {
        let mut token_bytes = [0_u8; 16];
        rand::rng().fill_bytes(&mut token_bytes);
        let delivery_token = hex::encode(token_bytes);
        self.send(wpb::AttemptResult {
            attempt_id: attempt_id.to_owned(),
            outputs: vec![wpb::AttemptOutput {
                manifest: pb::ArtifactManifest {
                    size_bytes: u64::try_from(bytes.len())?,
                    digest_sha256: ContentHash::digest(bytes).to_hex(),
                    kind: kind.into(),
                    mime_type: mime_type.to_owned(),
                    ..Default::default()
                }
                .into(),
                placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
                delivery_token: delivery_token.clone(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await?;
        Ok(delivery_token)
    }

    /// Reports that `attempt_id` failed with `kind` (ADR 0003).
    ///
    /// # Errors
    /// Returns an error if the send fails.
    pub async fn fail_attempt(
        &mut self,
        attempt_id: &str,
        kind: pb::FailureKind,
    ) -> anyhow::Result<()> {
        self.send(wpb::AttemptFailure {
            attempt_id: attempt_id.to_owned(),
            failure: pb::Failure {
                kind: kind.into(),
                message: "synthetic failure".to_owned(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .await
    }

    /// Downloads the bytes of input Artifact `artifact_id` for `attempt_id`
    /// over `WorkerTransferService.FetchArtifact`, as a Worker does for a
    /// `remote_local` input (ADR 0008).
    ///
    /// # Errors
    /// Returns an error if the RPC fails or the stream breaks.
    pub async fn fetch_input(
        &self,
        attempt_id: &str,
        artifact_id: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let transport = shared_transport(&self.remote_uri).await?;
        let config = ClientConfig::new(self.remote_uri.clone())
            .with_protocol(Protocol::Grpc)
            .with_default_header("authorization", format!("Bearer {}", self.credential));
        let client = wpb::WorkerTransferServiceClient::new(transport, config);
        let mut stream = client
            .fetch_artifact_with_options(
                wpb::FetchArtifactRequest {
                    artifact_id: artifact_id.to_owned(),
                    attempt_id: attempt_id.to_owned(),
                    offset: 0,
                    ..Default::default()
                },
                CallOptions::default(),
            )
            .await
            .map_err(|err| anyhow::anyhow!("FetchArtifact failed: {err}"))?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream
            .message::<wpb::ArtifactChunk>()
            .await
            .map_err(|err| anyhow::anyhow!("FetchArtifact stream error: {err}"))?
        {
            let chunk = chunk.to_owned_message();
            bytes.extend_from_slice(&chunk.data);
            if chunk.last {
                break;
            }
        }
        Ok(bytes)
    }

    /// Waits for Remote's `DeliverRequest` naming `delivery_token`, asking this
    /// Worker to upload a worker-local output (ADR 0008).
    ///
    /// # Errors
    /// Returns an error if the session errors or `timeout` elapses first.
    pub async fn recv_deliver(
        &mut self,
        timeout: Duration,
        delivery_token: &str,
    ) -> anyhow::Result<wpb::DeliverRequest> {
        self.recv_until(timeout, |message| match message.message {
            Some(remote_message::Message::Deliver(deliver))
                if deliver.delivery_token == delivery_token =>
            {
                Some(*deliver)
            }
            _ => None,
        })
        .await
    }

    /// Uploads `bytes` for `request` over `WorkerTransferService.DeliverArtifact`,
    /// declaring the same manifest `complete_attempt` committed to.
    ///
    /// # Errors
    /// Returns an error if the RPC fails.
    pub async fn deliver_output(
        &self,
        request: &wpb::DeliverRequest,
        bytes: &[u8],
        kind: pb::MediaKind,
        mime_type: &str,
    ) -> anyhow::Result<wpb::DeliverArtifactResponse> {
        let transport = shared_transport(&self.remote_uri).await?;
        let config = ClientConfig::new(self.remote_uri.clone())
            .with_protocol(Protocol::Grpc)
            .with_default_header("authorization", format!("Bearer {}", self.credential));
        let client = wpb::WorkerTransferServiceClient::new(transport, config);
        let manifest = pb::ArtifactManifest {
            size_bytes: u64::try_from(bytes.len())?,
            digest_sha256: ContentHash::digest(bytes).to_hex(),
            kind: kind.into(),
            mime_type: mime_type.to_owned(),
            ..Default::default()
        };
        let requests = vec![
            wpb::DeliverArtifactRequest {
                message: Some(
                    wpb::DeliverArtifactStart {
                        artifact_id: request.artifact_id.clone(),
                        delivery_token: request.delivery_token.clone(),
                        manifest: manifest.into(),
                        offset: request.offset,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            },
            wpb::DeliverArtifactRequest {
                message: Some(
                    wpb::ArtifactChunk {
                        offset: request.offset,
                        data: bytes.to_vec(),
                        last: true,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            },
        ];
        let response = client
            .deliver_artifact(connectrpc::stream_iter(requests))
            .await
            .map_err(|err| anyhow::anyhow!("DeliverArtifact failed: {err}"))?
            .into_owned();
        Ok(response)
    }
}
