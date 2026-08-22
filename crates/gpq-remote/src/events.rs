//! In-process fanout of live Generation events to Native API subscribers,
//! and (ADR 0008) persistence of the durable subset before fanout.
//!
//! ADR 0006: a reconnected Native event stream begins with the current
//! snapshot and then live events; token deltas are not replayed, and
//! discontinuity is explicit. [`EventHub::record`] is the single choke
//! point every Generation event publication should go through: it appends
//! the persistable form of `event` (`State`/`Progress`; ADR 0008 excludes
//! token deltas from durable storage, and an output Artifact's own row is
//! already durable) through [`crate::db::events::append`] and then fans it
//! out live via [`EventHub::publish`]. The current-snapshot-then-replay
//! sequencing itself lives in the Native Generation service
//! (`crate::native::generation`), which reads the row, replays anything
//! [`crate::db::events::load_since`] a captured boundary, and then
//! subscribes here for what comes next.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Context;
use gpq_domain::{FailureKind, GenerationId, GenerationState, TenantId};
use tokio::sync::broadcast;

use crate::db::Db;
use crate::db::events::{EventKind, EventRow};

/// Per-subscriber channel capacity. A slow subscriber that falls behind by
/// more than this many events observes a lag (`RecvError::Lagged`) rather
/// than blocking the publisher; ADR 0006 already treats a stream gap as an
/// explicit discontinuity, so this is an acceptable, self-describing failure
/// mode.
const CHANNEL_CAPACITY: usize = 256;

/// One live update about a Generation, published as it happens.
#[derive(Clone, Debug)]
pub enum GenerationEvent {
    /// The Generation transitioned to `state`. `attempt_count` and, for a
    /// failure, the classified cause accompany the transition so a
    /// subscriber never has to re-fetch the row just to render an update.
    State {
        /// The Generation's new state.
        state: GenerationState,
        /// How many Attempts have been created so far.
        attempt_count: u32,
        /// The classified failure, present only when `state` is `Failed`.
        failure: Option<(FailureKind, String)>,
    },
    /// A backend-reported progress update (ADR 0007).
    Progress {
        /// Fraction complete, `0.0..=1.0`.
        fraction: f64,
        /// A human-readable stage name.
        stage: String,
        /// The current step, for step-counted backends.
        step: u32,
        /// The total number of steps, for step-counted backends.
        total_steps: u32,
    },
    /// One incremental piece of streamed LLM output. Never replayed on
    /// reconnect (ADR 0006).
    Token {
        /// The token text.
        text: String,
    },
    /// An output Artifact became available for download. Carries no
    /// payload: the wire protocol has no partial-output update, so a
    /// `WatchGeneration` watcher (the only consumer) always responds by
    /// refetching and emitting a fresh full snapshot rather than reading
    /// anything off this event (ADR 0006, ADR 0008).
    Output,
    /// A gap in the event stream that a subscriber must not paper over,
    /// e.g. after a lagged reconnect (ADR 0006: "discontinuity is explicit").
    Discontinuity {
        /// A human-readable explanation of the gap.
        reason: String,
    },
}

/// JSON shape of a persisted `state_changed` row's payload.
#[derive(serde::Serialize, serde::Deserialize)]
struct StateChangedPayload {
    state: GenerationState,
    attempt_count: u32,
    failure: Option<FailurePayload>,
}

/// JSON shape of a persisted failure inside a `state_changed` payload.
#[derive(serde::Serialize, serde::Deserialize)]
struct FailurePayload {
    kind: FailureKind,
    message: String,
}

/// JSON shape of a persisted `progress` row's payload.
#[derive(serde::Serialize, serde::Deserialize)]
struct ProgressPayload {
    fraction: f64,
    stage: String,
    step: u32,
    total_steps: u32,
}

/// The persisted `(kind, payload)` for `event`, or `None` for the two
/// variants ADR 0008 excludes from durable storage: `Token` (token deltas
/// are explicitly not retained) and `Output` (the Artifact row it names is
/// already the durable record).
///
/// # Errors
/// Returns an error if `event`'s fields cannot be encoded as JSON (in
/// practice, only a non-finite `Progress::fraction` can fail this way).
fn persisted_form(
    event: &GenerationEvent,
) -> Result<Option<(EventKind, serde_json::Value)>, serde_json::Error> {
    match event {
        GenerationEvent::State {
            state,
            attempt_count,
            failure,
        } => {
            let payload = serde_json::to_value(StateChangedPayload {
                state: *state,
                attempt_count: *attempt_count,
                failure: failure
                    .clone()
                    .map(|(kind, message)| FailurePayload { kind, message }),
            })?;
            Ok(Some((EventKind::StateChanged, payload)))
        }
        GenerationEvent::Progress {
            fraction,
            stage,
            step,
            total_steps,
        } => {
            let payload = serde_json::to_value(ProgressPayload {
                fraction: *fraction,
                stage: stage.clone(),
                step: *step,
                total_steps: *total_steps,
            })?;
            Ok(Some((EventKind::Progress, payload)))
        }
        GenerationEvent::Token { .. }
        | GenerationEvent::Output
        | GenerationEvent::Discontinuity { .. } => Ok(None),
    }
}

/// Decodes a persisted row back into the live event it represents.
///
/// `None` for an `attempt_created` row (ADR 0008 keeps it purely as audit
/// history — it has no `GenerationEvent` counterpart and a `WatchGeneration`
/// replay never surfaces it) or for a payload that fails to parse (a
/// corrupt or foreign-written row is dropped rather than breaking the whole
/// replay).
pub(crate) fn decode_persisted(row: &EventRow) -> Option<GenerationEvent> {
    if row.kind == EventKind::StateChanged.as_str() {
        let payload: StateChangedPayload = serde_json::from_value(row.payload.clone()).ok()?;
        return Some(GenerationEvent::State {
            state: payload.state,
            attempt_count: payload.attempt_count,
            failure: payload.failure.map(|f| (f.kind, f.message)),
        });
    }
    if row.kind == EventKind::Progress.as_str() {
        let payload: ProgressPayload = serde_json::from_value(row.payload.clone()).ok()?;
        return Some(GenerationEvent::Progress {
            fraction: payload.fraction,
            stage: payload.stage,
            step: payload.step,
            total_steps: payload.total_steps,
        });
    }
    None
}

/// Appends `event`'s persisted form inside its own tenant-scoped
/// transaction (`crate::db::events::append`'s locking contract).
async fn append_persisted(
    db: &Db,
    tenant: TenantId,
    generation: GenerationId,
    kind: EventKind,
    payload: serde_json::Value,
) -> anyhow::Result<()> {
    let mut conn = db
        .begin_tenant(tenant)
        .await
        .context("failed to open a tenant transaction to persist a Generation event")?;
    crate::db::events::append(&mut conn, tenant, generation, kind, payload)
        .await
        .context("failed to append a Generation event")?;
    conn.commit()
        .await
        .context("failed to commit a persisted Generation event")
}

/// In-process broadcast hub keyed by Generation. Holds no durable state:
/// everything here is lost on restart, which is fine because it only ever
/// carries live, already-in-flight updates.
#[derive(Clone, Default)]
pub struct EventHub {
    channels: std::sync::Arc<Mutex<HashMap<GenerationId, broadcast::Sender<GenerationEvent>>>>,
}

impl EventHub {
    /// Publishes `event` for `generation`. A no-op if nobody is subscribed —
    /// publishing never blocks and never errors, and a channel with no
    /// remaining receivers is pruned so the map does not grow unbounded.
    pub fn publish(&self, generation: GenerationId, event: GenerationEvent) {
        let mut channels = channels_lock(&self.channels);
        let Some(sender) = channels.get(&generation) else {
            return;
        };
        if sender.receiver_count() == 0 {
            channels.remove(&generation);
            return;
        }
        // `send` only fails when every receiver has been dropped, which the
        // `receiver_count()` check above already ruled out for the common
        // case; a receiver dropped between the check and here is still a
        // harmless no-op.
        let _ = sender.send(event);
    }

    /// Persists the durable subset of `event` (ADR 0008: `State` and
    /// `Progress` transitions) and then fans it out live via
    /// [`EventHub::publish`] — the single choke point every Generation
    /// event publication should go through, so that ADR 0008's "state
    /// transitions and progress snapshots are retained" holds no matter
    /// which caller reports the transition. `Token` and `Output` are never
    /// persisted (ADR 0008): they are published live only, identically to
    /// calling [`EventHub::publish`] directly.
    ///
    /// If the append itself fails, `event` is never published as if nothing
    /// happened — that would let a client believe a transition it can never
    /// replay after reconnecting simply did not occur. Instead a
    /// `Discontinuity` is published in its place (ADR 0006: "discontinuity
    /// is explicit") before the error is returned.
    ///
    /// # Errors
    /// Returns an error if `event` cannot be encoded or the append cannot
    /// be committed.
    pub async fn record(
        &self,
        db: &Db,
        tenant: TenantId,
        generation: GenerationId,
        event: &GenerationEvent,
    ) -> anyhow::Result<()> {
        let persisted = persisted_form(event)
            .with_context(|| format!("failed to encode {event:?} for persistence"))?;
        if let Some((kind, payload)) = persisted
            && let Err(err) = append_persisted(db, tenant, generation, kind, payload).await
        {
            self.publish(
                generation,
                GenerationEvent::Discontinuity {
                    reason: "failed to persist a Generation event".to_owned(),
                },
            );
            return Err(err);
        }
        self.publish(generation, event.clone());
        Ok(())
    }

    /// Subscribes to live events for `generation`, creating its channel if
    /// this is the first subscriber.
    #[must_use]
    pub fn subscribe(&self, generation: GenerationId) -> broadcast::Receiver<GenerationEvent> {
        let mut channels = channels_lock(&self.channels);
        channels
            .entry(generation)
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .subscribe()
    }
}

fn channels_lock(
    channels: &Mutex<HashMap<GenerationId, broadcast::Sender<GenerationEvent>>>,
) -> std::sync::MutexGuard<'_, HashMap<GenerationId, broadcast::Sender<GenerationEvent>>> {
    channels
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_subscriber_receives_a_published_event() {
        let hub = EventHub::default();
        let generation = GenerationId::new();
        let mut receiver = hub.subscribe(generation);

        hub.publish(
            generation,
            GenerationEvent::State {
                state: GenerationState::Running,
                attempt_count: 1,
                failure: None,
            },
        );

        let Ok(GenerationEvent::State {
            state,
            attempt_count,
            failure,
        }) = receiver.recv().await
        else {
            panic!("expected the published State event to be delivered")
        };
        assert_eq!(state, GenerationState::Running);
        assert_eq!(attempt_count, 1);
        assert!(failure.is_none());
    }

    #[test]
    fn publishing_with_no_subscriber_does_not_error() {
        let hub = EventHub::default();
        hub.publish(
            GenerationId::new(),
            GenerationEvent::Discontinuity {
                reason: "test".to_string(),
            },
        );
    }

    fn sample_row(kind: EventKind, payload: serde_json::Value) -> EventRow {
        EventRow {
            sequence: 1,
            kind: kind.as_str().to_owned(),
            payload,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn state_events_round_trip_through_persisted_form_and_decode() {
        let event = GenerationEvent::State {
            state: GenerationState::Failed,
            attempt_count: 2,
            failure: Some((FailureKind::OutOfMemory, "oom".to_owned())),
        };
        let Ok(Some((kind, payload))) = persisted_form(&event) else {
            panic!("State events must be persistable");
        };
        assert_eq!(kind.as_str(), "state_changed");
        let Some(GenerationEvent::State {
            state,
            attempt_count,
            failure,
        }) = decode_persisted(&sample_row(kind, payload))
        else {
            panic!("expected a decoded State event");
        };
        assert_eq!(state, GenerationState::Failed);
        assert_eq!(attempt_count, 2);
        let Some((failure_kind, message)) = failure else {
            panic!("expected a decoded failure");
        };
        assert_eq!(failure_kind, FailureKind::OutOfMemory);
        assert_eq!(message, "oom");
    }

    #[test]
    fn progress_events_round_trip_through_persisted_form_and_decode() {
        let event = GenerationEvent::Progress {
            fraction: 0.5,
            stage: "denoise".to_owned(),
            step: 3,
            total_steps: 10,
        };
        let Ok(Some((kind, payload))) = persisted_form(&event) else {
            panic!("Progress events must be persistable");
        };
        assert_eq!(kind.as_str(), "progress");
        let Some(GenerationEvent::Progress {
            fraction,
            stage,
            step,
            total_steps,
        }) = decode_persisted(&sample_row(kind, payload))
        else {
            panic!("expected a decoded Progress event");
        };
        assert!((fraction - 0.5).abs() < f64::EPSILON);
        assert_eq!(stage, "denoise");
        assert_eq!(step, 3);
        assert_eq!(total_steps, 10);
    }

    #[test]
    fn token_deltas_are_never_persisted() {
        let event = GenerationEvent::Token {
            text: "hi".to_owned(),
        };
        assert_eq!(persisted_form(&event).ok(), Some(None));
    }

    #[test]
    fn outputs_are_never_persisted() {
        assert_eq!(persisted_form(&GenerationEvent::Output).ok(), Some(None));
    }

    #[test]
    fn discontinuities_are_never_persisted() {
        let event = GenerationEvent::Discontinuity {
            reason: "lagged".to_owned(),
        };
        assert_eq!(persisted_form(&event).ok(), Some(None));
    }

    #[test]
    fn attempt_created_rows_have_no_generation_event_counterpart() {
        let row = sample_row(
            EventKind::AttemptCreated,
            serde_json::json!({ "attempt_number": 1 }),
        );
        assert!(decode_persisted(&row).is_none());
    }

    #[test]
    fn a_malformed_payload_is_dropped_rather_than_panicking() {
        let row = sample_row(
            EventKind::Progress,
            serde_json::json!({ "unexpected": true }),
        );
        assert!(decode_persisted(&row).is_none());
    }
}
