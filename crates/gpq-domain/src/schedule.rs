//! GPU-utilization-first scheduling order.
//!
//! Selection excludes Generations incompatible with the Tenant, pinned model,
//! selected `LoRAs`, or slot shape limits. It then chooses overdue work
//! by oldest creation time; otherwise it favors the resident model, priority,
//! and oldest submission. Running Attempts are never preempted.
use chrono::{DateTime, Utc};

use crate::capability::{Requirement, SlotCapability};
use crate::generation::Priority;
use crate::hash::ContentHash;
use crate::id::GenerationId;
use crate::tenant::TenantSettings;

/// A queued Generation offered to the scheduler.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Candidate {
    /// Identity of the queued Generation.
    pub generation_id: GenerationId,
    /// Submission time, from database time (ADR 0013).
    pub created_at: DateTime<Utc>,
    /// Requested priority.
    pub priority: Priority,
    /// What the Generation needs from a Slot.
    pub requirement: Requirement,
}

impl Candidate {
    /// How long the Generation has been queued at `now`.
    #[must_use]
    pub fn age(&self, now: DateTime<Utc>) -> std::time::Duration {
        (now - self.created_at).to_std().unwrap_or_default()
    }

    /// Pinned version hash, used to group batchable work.
    #[must_use]
    pub const fn version(&self) -> ContentHash {
        self.requirement.version
    }
}

/// The Slot asking for work.
#[derive(Clone, Debug)]
pub struct SlotContext<'a> {
    /// What the Slot advertises.
    pub capability: &'a SlotCapability,
    /// Free Execution Slots on the Active Runtime; Workers never prefetch
    /// beyond this (ADR 0005).
    pub free_slots: u32,
    /// Tenant policy driving the starvation guard.
    pub settings: TenantSettings,
    /// Database time.
    pub now: DateTime<Utc>,
}

impl SlotContext<'_> {
    fn eligible(&self, candidate: &Candidate) -> bool {
        self.capability.admits(&candidate.requirement).is_ok()
    }

    fn is_overdue(&self, candidate: &Candidate) -> bool {
        self.settings.is_overdue(candidate.age(self.now))
    }
}

/// Picks the next Generation this Slot should execute.
///
/// Returns `None` when the Slot has no free capacity or nothing compatible is
/// queued.
#[must_use]
pub fn select_next<'a>(
    context: &SlotContext<'_>,
    candidates: &'a [Candidate],
) -> Option<&'a Candidate> {
    if context.free_slots == 0 {
        return None;
    }

    let eligible = || candidates.iter().filter(|c| context.eligible(c));

    // Starvation guard first: overdue work ignores cache affinity and priority.
    let overdue = eligible()
        .filter(|c| context.is_overdue(c))
        .min_by_key(|c| (c.created_at, c.generation_id));
    if overdue.is_some() {
        return overdue;
    }

    eligible().min_by_key(|c| {
        (
            // Resident-model work first: it avoids a model load.
            u8::from(!context.capability.holds_resident_model(&c.requirement)),
            // Then higher priority.
            Priority::MAX.get() - c.priority.get(),
            // Then oldest submission.
            c.created_at,
            c.generation_id,
        )
    })
}

/// Picks a batch of Generations for one Slot request.
///
/// The first element is [`select_next`]'s choice; the rest share its pinned
/// version so a continuous-batching runtime can execute them without switching
/// models, bounded by the Slot's free capacity (ADR 0002, ADR 0005).
#[must_use]
pub fn select_batch<'a>(
    context: &SlotContext<'_>,
    candidates: &'a [Candidate],
) -> Vec<&'a Candidate> {
    let Some(primary) = select_next(context, candidates) else {
        return Vec::new();
    };

    let capacity = context.free_slots as usize;
    let mut batch = Vec::with_capacity(capacity);
    batch.push(primary);

    let mut companions: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| {
            c.generation_id != primary.generation_id
                && c.version() == primary.version()
                && context.eligible(c)
        })
        .collect();
    companions.sort_unstable_by_key(|c| {
        (
            Priority::MAX.get() - c.priority.get(),
            c.created_at,
            c.generation_id,
        )
    });
    batch.extend(companions.into_iter().take(capacity - 1));
    batch
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use super::*;
    use crate::capability::ShapeLimits;
    use crate::generation::{GenerationParameters, ImageParameters};
    use crate::id::{DevicePoolId, SlotId, TenantId, WorkerId};
    use crate::version::{ModelAssetRole, ModelFamily, ModelManifest};

    fn manifest(seed: &[u8]) -> ModelManifest {
        let hash = ContentHash::digest(seed);
        ModelManifest {
            family: ModelFamily::QwenImage21,
            assets: BTreeMap::from([
                (ModelAssetRole::Diffusion, hash),
                (ModelAssetRole::TextEncoder, hash),
                (ModelAssetRole::VisionEncoder, hash),
                (ModelAssetRole::Vae, hash),
            ]),
            default_loras: Vec::new(),
            sampling: ModelFamily::QwenImage21.default_sampling_preset(),
        }
    }

    fn model_hash(seed: &[u8]) -> ContentHash {
        manifest(seed)
            .canonical_hash()
            .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"))
    }

    struct Fixture {
        tenant: TenantId,
        capability: SlotCapability,
        now: DateTime<Utc>,
    }

    impl Fixture {
        fn new() -> Self {
            let tenant = TenantId::new();
            let a = manifest(b"a");
            let b = manifest(b"b");
            let a_hash = a
                .canonical_hash()
                .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"));
            let b_hash = b
                .canonical_hash()
                .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"));
            Self {
                tenant,
                capability: SlotCapability {
                    tenant_id: tenant,
                    worker_id: WorkerId::new(),
                    pool_id: DevicePoolId::new(),
                    slot_id: SlotId::new(),
                    accelerator: "metal".to_owned(),
                    accelerator_version: "native-test".to_owned(),
                    model_manifests: BTreeMap::from([(a_hash, a), (b_hash, b)]),
                    image_loras: BTreeSet::new(),
                    resident_model: Some(a_hash),
                    accelerator_memory_bytes: None,
                    shape_limits: ShapeLimits::default(),
                },
                now: DateTime::from_timestamp_secs(1_700_000_000).unwrap_or_else(Utc::now),
            }
        }

        fn context(&self, free_slots: u32) -> SlotContext<'_> {
            SlotContext {
                capability: &self.capability,
                free_slots,
                settings: TenantSettings::default(),
                now: self.now,
            }
        }

        fn candidate(&self, seed: &[u8], priority: u8, age_secs: i64) -> Candidate {
            let model = manifest(seed);
            let version = model
                .canonical_hash()
                .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"));
            let parameters = GenerationParameters::Image(ImageParameters {
                prompt: "a landscape".to_owned(),
                negative_prompt: String::new(),
                width: 512,
                height: 512,
                steps: None,
                cfg_scale: None,
                references: Vec::new(),
                loras: Vec::new(),
                sigmas: None,
            });
            Candidate {
                generation_id: GenerationId::new(),
                created_at: self.now - chrono::Duration::seconds(age_secs),
                priority: Priority::new(priority).unwrap_or(Priority::DEFAULT),
                requirement: Requirement::for_generation(self.tenant, version, &model, &parameters)
                    .unwrap_or_else(|error| panic!("fixture request is valid: {error}")),
            }
        }
    }

    #[test]
    fn no_free_slot_means_no_work() {
        let fixture = Fixture::new();
        let candidates = vec![fixture.candidate(b"a", 5, 10)];
        assert!(select_next(&fixture.context(0), &candidates).is_none());
    }

    #[test]
    fn incompatible_work_is_excluded() {
        let fixture = Fixture::new();
        let candidates = vec![fixture.candidate(b"missing", 9, 100)];
        assert!(select_next(&fixture.context(1), &candidates).is_none());
    }

    #[test]
    fn resident_model_beats_higher_priority() {
        let fixture = Fixture::new();
        let resident = fixture.candidate(b"a", 1, 10);
        let urgent_other_model = fixture.candidate(b"b", 9, 10);
        let candidates = vec![urgent_other_model, resident.clone()];
        assert_eq!(
            select_next(&fixture.context(1), &candidates),
            Some(&candidates[1]),
            "cache affinity outranks priority for non-overdue work"
        );
        assert_eq!(candidates[1].version(), resident.version());
    }

    #[test]
    fn priority_breaks_ties_within_the_resident_model() {
        let fixture = Fixture::new();
        let low = fixture.candidate(b"a", 2, 10);
        let high = fixture.candidate(b"a", 8, 5);
        let candidates = vec![low, high];
        assert_eq!(
            select_next(&fixture.context(1), &candidates),
            Some(&candidates[1])
        );
    }

    #[test]
    fn age_breaks_ties_within_equal_priority() {
        let fixture = Fixture::new();
        let newer = fixture.candidate(b"a", 5, 5);
        let older = fixture.candidate(b"a", 5, 500);
        let candidates = vec![newer, older];
        assert_eq!(
            select_next(&fixture.context(1), &candidates),
            Some(&candidates[1])
        );
    }

    #[test]
    fn overdue_work_wins_by_oldest_creation() {
        let fixture = Fixture::new();
        let resident_fresh = fixture.candidate(b"a", 9, 30);
        let overdue_other_model = fixture.candidate(b"b", 0, 3600);
        let candidates = vec![resident_fresh, overdue_other_model];
        assert_eq!(
            select_next(&fixture.context(1), &candidates),
            Some(&candidates[1]),
            "maximum_queue_age must defeat cache affinity and priority"
        );
    }

    #[test]
    fn overdue_selection_ignores_priority() {
        let fixture = Fixture::new();
        let overdue_urgent = fixture.candidate(b"a", 9, 1900);
        let overdue_oldest = fixture.candidate(b"b", 0, 4000);
        let candidates = vec![overdue_urgent, overdue_oldest];
        assert_eq!(
            select_next(&fixture.context(1), &candidates),
            Some(&candidates[1])
        );
    }

    #[test]
    fn queue_age_boundary_follows_tenant_settings() {
        let fixture = Fixture::new();
        let mut context = fixture.context(1);
        context.settings.maximum_queue_age = Duration::from_mins(1);
        let resident = fixture.candidate(b"a", 9, 10);
        let other = fixture.candidate(b"b", 0, 61);
        let candidates = vec![resident, other];
        assert_eq!(select_next(&context, &candidates), Some(&candidates[1]));

        context.settings.maximum_queue_age = Duration::from_mins(10);
        assert_eq!(select_next(&context, &candidates), Some(&candidates[0]));
    }

    #[test]
    fn batches_share_the_pinned_version_and_respect_capacity() {
        let fixture = Fixture::new();
        let first = fixture.candidate(b"a", 5, 100);
        let second = fixture.candidate(b"a", 5, 50);
        let third = fixture.candidate(b"a", 5, 10);
        let other_model = fixture.candidate(b"b", 9, 10);
        let candidates = vec![first, second, third, other_model];

        let batch = select_batch(&fixture.context(3), &candidates);
        assert_eq!(batch.len(), 3);
        assert!(batch.iter().all(|c| c.version() == model_hash(b"a")));
        assert_eq!(batch[0], &candidates[0], "oldest resident work leads");
        assert_eq!(batch[1], &candidates[1]);

        let single = select_batch(&fixture.context(1), &candidates);
        assert_eq!(single.len(), 1, "never prefetch beyond free slots");
    }
}
