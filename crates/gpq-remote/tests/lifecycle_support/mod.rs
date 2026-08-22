//! A synthetic Worker: drives `WorkerEnrollmentService`/`WorkerSessionService`
//! directly with the generated `connectrpc` client (ADR 0004), impersonating
//! a Worker at the protocol layer.
//!
//! `lifecycle.rs` needs this for two things the real `gpq-worker` binary can
//! never produce:
//!
//! - A `Handshake` carrying a deliberately incompatible protocol major (ADR
//!   0004) — the real Worker only ever sends its own, compatible version.
//! - A late/stale `AttemptResult` racing a cancellation or an expired lease
//!   (ADR 0003) — `crates/gpq-worker/src/backend/llama.rs` drops the backend
//!   connection the instant its `CancellationToken` fires, so a real Worker
//!   can only ever send `CancelAcknowledged` once cancellation starts, never
//!   a late success for the same Attempt.
//!
//! Each synthetic Worker advertises its own llama.cpp Pool resident on a
//! freshly generated, otherwise-unused Model Version, so Remote's scheduler
//! never routes a Generation meant for one synthetic Worker (or the shared
//! harness's real one) to another.

use std::time::Duration;

use anyhow::{Context, bail};
use connectrpc::client::{ClientConfig, Http2Connection};
use connectrpc::{ConnectError, Protocol};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use gpq_proto::gpq::worker::v1::__buffa::oneof::remote_message;
use rand::RngCore;

use crate::e2e_support::{Harness, wait_until};

/// The bidirectional control stream a synthetic Worker holds for its whole
/// lifetime, typed concretely for the plaintext HTTP/2 transport every
/// harness `gpq-remote serve` speaks.
type SyntheticStream = connectrpc::client::BidiStream<
    hyper::body::Incoming,
    wpb::WorkerMessage,
    <wpb::RemoteMessage as buffa::HasMessageView>::View<'static>,
>;

/// Every synthetic Worker name is prefixed with this, so a test can pick the
/// harness's real Worker back out of a `ListWorkers` response by exclusion
/// without needing a name the harness never exposes.
pub const NAME_PREFIX: &str = "lifecycle-synthetic-";

fn random_content_sha256() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

async fn shared_transport(
    remote_uri: &http::Uri,
) -> anyhow::Result<connectrpc::client::SharedHttp2Connection> {
    let connection = Http2Connection::connect_plaintext(remote_uri.clone())
        .await
        .context("connecting to remote for a synthetic worker")?;
    Ok(connection.shared(16))
}

/// Enrolls `worker_name` under `harness`'s tenant 1 with the real protocol
/// version, returning its worker id and Worker Credential (ADR 0009).
async fn enroll(harness: &Harness, worker_name: &str) -> anyhow::Result<(String, String)> {
    let remote_uri: http::Uri = harness
        .url("")
        .parse()
        .context("parsing the remote base uri")?;
    let transport = shared_transport(&remote_uri).await?;
    let config = ClientConfig::new(remote_uri)
        .with_protocol(Protocol::Grpc)
        .with_default_header(
            "authorization",
            format!("Bearer {}", harness.tenant1.master_key),
        );
    let client = wpb::WorkerEnrollmentServiceClient::new(transport, config);
    let request = wpb::EnrollRequest {
        worker_name: worker_name.to_owned(),
        host_descriptor: "gpq-lifecycle-synthetic".to_owned(),
        protocol_major: gpq_proto::PROTOCOL_MAJOR,
        protocol_minor: gpq_proto::PROTOCOL_MINOR,
        worker_version: "lifecycle-synthetic".to_owned(),
        ..Default::default()
    };
    let response = client
        .enroll(request)
        .await
        .map_err(|err| anyhow::anyhow!("synthetic worker Enroll failed: {err}"))?
        .into_owned();
    Ok((response.worker_id, response.worker_credential))
}

/// Opens the Worker control Session and sends a `Handshake` reporting
/// `protocol_major`, returning the stream and `HandshakeAck` on success or
/// the `ConnectError` Remote rejected it with (ADR 0004's explicit
/// major-mismatch rejection is exactly what a bad `protocol_major` proves).
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
                    worker_version: "lifecycle-synthetic".to_owned(),
                    host_descriptor: "gpq-lifecycle-synthetic".to_owned(),
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

/// A Worker impersonated at the protocol layer over its real control
/// Session, so a test fully controls what it leases, heartbeats, and
/// reports back.
pub struct SyntheticWorker {
    stream: SyntheticStream,
}

impl SyntheticWorker {
    /// Enrolls `worker_name` (which must start with [`NAME_PREFIX`]) and
    /// completes the handshake with the real protocol version.
    ///
    /// # Errors
    /// Returns an error if enrollment or the handshake fails.
    pub async fn connect(harness: &Harness, worker_name: &str) -> anyhow::Result<Self> {
        debug_assert!(worker_name.starts_with(NAME_PREFIX));
        let remote_uri: http::Uri = harness
            .url("")
            .parse()
            .context("parsing the remote base uri")?;
        let (_worker_id, credential) = enroll(harness, worker_name).await?;
        let (stream, _ack) = open_session(&remote_uri, &credential, gpq_proto::PROTOCOL_MAJOR)
            .await?
            .map_err(|err| anyhow::anyhow!("synthetic worker handshake failed: {err}"))?;
        Ok(Self { stream })
    }

    /// Enrolls `worker_name` and attempts the handshake with a deliberately
    /// wrong `protocol_major`, returning the `ConnectError` Remote rejects
    /// it with rather than a live [`SyntheticWorker`] (ADR 0004).
    ///
    /// # Errors
    /// Returns an error if enrollment fails or the handshake unexpectedly
    /// succeeds.
    pub async fn connect_with_bad_protocol_major(
        harness: &Harness,
        worker_name: &str,
        protocol_major: u32,
    ) -> anyhow::Result<ConnectError> {
        debug_assert!(worker_name.starts_with(NAME_PREFIX));
        let remote_uri: http::Uri = harness
            .url("")
            .parse()
            .context("parsing the remote base uri")?;
        let (_worker_id, credential) = enroll(harness, worker_name).await?;
        match open_session(&remote_uri, &credential, protocol_major).await? {
            Ok(_) => bail!(
                "synthetic worker handshake unexpectedly succeeded with protocol_major {protocol_major}"
            ),
            Err(err) => Ok(err),
        }
    }

    /// Sends one `WorkerMessage` over the control Session.
    ///
    /// # Errors
    /// Returns an error if the send fails.
    pub async fn send(
        &mut self,
        message: impl Into<wpb::__buffa::oneof::worker_message::Message>,
    ) -> anyhow::Result<()> {
        self.stream
            .send(wpb::WorkerMessage {
                message: Some(message.into()),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("synthetic worker send failed: {err}"))
    }

    /// Reads `RemoteMessage`s until `matches` returns `Some`, discarding
    /// every message it rejects, or `timeout` elapses.
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
            let next = tokio::time::timeout(remaining, self.stream.message::<wpb::RemoteMessage>())
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

    /// Waits for the next `DiscardOutput` whose `delivery_token` matches
    /// `delivery_token`.
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
}

/// A [`SyntheticWorker`] advertising one ready llama.cpp Pool resident on a
/// freshly generated Model Version, plus the Model alias only it serves.
pub struct SyntheticLlmWorker {
    pub worker: SyntheticWorker,
    pub alias: String,
}

impl SyntheticLlmWorker {
    /// Enrolls, connects, advertises capability, and aliases a private
    /// Model Version, so `harness.native_submit_model(&alias)` schedules
    /// exclusively onto this synthetic Worker (ADR 0012).
    ///
    /// # Errors
    /// Returns an error if enrollment, the handshake, capability
    /// registration, or aliasing fails.
    pub async fn spawn(harness: &Harness, worker_name: &str) -> anyhow::Result<Self> {
        debug_assert!(worker_name.starts_with(NAME_PREFIX));
        let model_sha256 = random_content_sha256();
        let mut worker = SyntheticWorker::connect(harness, worker_name).await?;

        worker
            .send(wpb::CapabilityReport {
                pools: vec![wpb::PoolAdvertisement {
                    pool_id: "synthetic-pool".to_owned(),
                    backend_kind: pb::BackendKind::BACKEND_KIND_LLAMA_CPP.into(),
                    backend_version: "synthetic".to_owned(),
                    ready: true,
                    slots: vec![wpb::SlotAdvertisement {
                        slot_id: "slot0".to_owned(),
                        busy: false,
                        attempt_id: String::new(),
                        ..Default::default()
                    }],
                    resident_model_sha256: model_sha256.clone(),
                    model_sha256: vec![model_sha256.clone()],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .context("advertising the synthetic worker's capability")?;

        wait_until(
            || async {
                let response = harness
                    .catalog_client(&harness.tenant1.master_key)
                    .list_workers(pb::ListWorkersRequest::default())
                    .await
                    .map_err(|err| anyhow::anyhow!("ListWorkers failed: {err}"))?
                    .into_owned();
                let ready = response.workers.iter().any(|worker| {
                    worker.name == worker_name
                        && worker.online
                        && worker.pools.iter().any(|pool| pool.total_slots > 0)
                });
                Ok(ready.then_some(()))
            },
            Duration::from_secs(30),
        )
        .await
        .context("waiting for the synthetic worker's Pool to register")?;

        let alias = format!("{worker_name}-alias");
        harness
            .catalog_client(&harness.tenant1.master_key)
            .set_model_alias(pb::SetModelAliasRequest {
                alias: alias.clone(),
                content_sha256: model_sha256,
                ..Default::default()
            })
            .await
            .map_err(|err| {
                anyhow::anyhow!("SetModelAlias for the synthetic worker failed: {err}")
            })?;

        Ok(Self { worker, alias })
    }
}
