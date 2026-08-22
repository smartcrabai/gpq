//! Capability matching between a Generation and an Execution Slot.
//!
//! ADR 0001 keeps every Slot inside one Tenant. ADR 0012 pins exact Model and
//! Workflow Versions, so a match is by content hash and never by alias. ADR 0005
//! makes accelerator memory optional, backend-derived telemetry: unknown memory
//! does not reject work, and a runtime OOM corrects the claim afterwards. ADR
//! 0007 matches the Workflow manifest's required Models and custom-node versions
//! against what the Worker advertises. ADR 0018 rejects workflows naming absent
//! custom nodes instead of installing them.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::hash::ContentHash;
use crate::id::{DevicePoolId, SlotId, TenantId, WorkerId};
use crate::modality::BackendKind;
use crate::version::WorkflowManifest;

/// What a Generation needs from a Slot in order to run.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Requirement {
    /// Owning Tenant.
    pub tenant_id: TenantId,
    /// Runtime kind that must be the Slot's Active Runtime.
    pub backend_kind: BackendKind,
    /// Pinned Model or Workflow Version hash.
    pub version: ContentHash,
    /// Model Versions the execution loads.
    pub required_models: BTreeSet<ContentHash>,
    /// Custom-node package name to exact required version.
    pub required_custom_nodes: BTreeMap<String, String>,
    /// Estimated accelerator memory, when the version declares one.
    pub estimated_vram_bytes: Option<u64>,
}

impl Requirement {
    /// Builds the requirement of a llama.cpp Model Version.
    #[must_use]
    pub fn for_model(tenant_id: TenantId, version: ContentHash, vram_bytes: Option<u64>) -> Self {
        Self {
            tenant_id,
            backend_kind: BackendKind::LlamaCpp,
            version,
            required_models: BTreeSet::from([version]),
            required_custom_nodes: BTreeMap::new(),
            estimated_vram_bytes: vram_bytes,
        }
    }

    /// Builds the requirement of a `ComfyUI` Workflow Version and its manifest.
    #[must_use]
    pub fn for_workflow(
        tenant_id: TenantId,
        version: ContentHash,
        manifest: &WorkflowManifest,
        vram_bytes: Option<u64>,
    ) -> Self {
        Self {
            tenant_id,
            backend_kind: BackendKind::ComfyUi,
            version,
            required_models: manifest.required_models.iter().copied().collect(),
            required_custom_nodes: manifest.required_custom_nodes.clone(),
            estimated_vram_bytes: vram_bytes,
        }
    }
}

/// What one Execution Slot advertises about itself.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct SlotCapability {
    /// Owning Tenant of the Worker.
    pub tenant_id: TenantId,
    /// The advertising Worker.
    pub worker_id: WorkerId,
    /// The Device Pool whose Active Runtime exposes this Slot.
    pub pool_id: DevicePoolId,
    /// Slot identity.
    pub slot_id: SlotId,
    /// Kind of the Active Runtime currently occupying the Pool.
    pub backend_kind: BackendKind,
    /// Observed backend version string.
    pub backend_version: String,
    /// Model Versions present on the host, by content hash.
    pub model_versions: BTreeSet<ContentHash>,
    /// Installed custom nodes, package name to exact version.
    pub custom_nodes: BTreeMap<String, String>,
    /// Model Version currently loaded in the Pool, if any.
    pub resident_model: Option<ContentHash>,
    /// Accelerator memory reported by the backend, when known.
    pub accelerator_memory_bytes: Option<u64>,
    /// Versions this Slot proved incapable of, e.g. by running out of memory.
    pub incapable_versions: BTreeSet<ContentHash>,
}

/// Why a Slot cannot run a Generation.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum IncapableReason {
    /// The Slot belongs to a different Tenant.
    #[error("slot belongs to another tenant")]
    TenantMismatch,
    /// The Slot's Active Runtime is the wrong kind.
    #[error("slot runs {found} but {required} is required")]
    BackendMismatch {
        /// Runtime kind required by the Generation.
        required: BackendKind,
        /// Runtime kind the Slot currently hosts.
        found: BackendKind,
    },
    /// A pinned Model Version is not present on the host.
    #[error("model version {0} is not installed")]
    MissingModel(ContentHash),
    /// A required custom node is absent or at the wrong version.
    #[error("custom node {name} requires {required}, found {found:?}")]
    MissingCustomNode {
        /// Package name.
        name: String,
        /// Version the Workflow manifest requires.
        required: String,
        /// Version installed on the host, if the package exists at all.
        found: Option<String>,
    },
    /// This Slot already failed the version for a candidate-specific reason.
    #[error("slot is known incapable of version {0}")]
    KnownIncapable(ContentHash),
    /// Known accelerator memory is smaller than the version's estimate.
    #[error("slot has {available} bytes of accelerator memory, {required} estimated")]
    InsufficientMemory {
        /// Estimated need.
        required: u64,
        /// Reported capacity.
        available: u64,
    },
}

impl SlotCapability {
    /// Decides whether this Slot may execute a Generation.
    ///
    /// Unknown accelerator memory never rejects work: ADR 0005 treats memory as
    /// optional telemetry and relies on runtime OOM to correct the claim.
    ///
    /// # Errors
    ///
    /// Returns the [`IncapableReason`] that excludes this Slot.
    pub fn admits(&self, requirement: &Requirement) -> Result<(), IncapableReason> {
        if self.tenant_id != requirement.tenant_id {
            return Err(IncapableReason::TenantMismatch);
        }
        if self.backend_kind != requirement.backend_kind {
            return Err(IncapableReason::BackendMismatch {
                required: requirement.backend_kind,
                found: self.backend_kind,
            });
        }
        if self.incapable_versions.contains(&requirement.version) {
            return Err(IncapableReason::KnownIncapable(requirement.version));
        }
        for model in &requirement.required_models {
            if !self.model_versions.contains(model) {
                return Err(IncapableReason::MissingModel(*model));
            }
        }
        for (name, required) in &requirement.required_custom_nodes {
            let found = self.custom_nodes.get(name);
            if found.map(String::as_str) != Some(required.as_str()) {
                return Err(IncapableReason::MissingCustomNode {
                    name: name.clone(),
                    required: required.clone(),
                    found: found.cloned(),
                });
            }
        }
        if let (Some(required), Some(available)) = (
            requirement.estimated_vram_bytes,
            self.accelerator_memory_bytes,
        ) && required > available
        {
            return Err(IncapableReason::InsufficientMemory {
                required,
                available,
            });
        }
        Ok(())
    }

    /// Whether the Slot already holds the requirement's Model in memory.
    ///
    /// Cache-aware scheduling prefers these Slots (ADR 0002).
    #[must_use]
    pub fn holds_resident_model(&self, requirement: &Requirement) -> bool {
        match self.resident_model {
            Some(resident) => match requirement.backend_kind {
                BackendKind::LlamaCpp => resident == requirement.version,
                BackendKind::ComfyUi => requirement.required_models.contains(&resident),
            },
            None => false,
        }
    }

    /// Records that this Slot proved incapable of a version.
    pub fn mark_incapable(&mut self, version: ContentHash) {
        self.incapable_versions.insert(version);
    }
}

/// Whether any registered Slot is still a candidate for a requirement.
///
/// A Generation fails for VRAM insufficiency only after every candidate is known
/// incapable (ADR 0003).
#[must_use]
pub fn any_candidate_remains<'a>(
    slots: impl IntoIterator<Item = &'a SlotCapability>,
    requirement: &Requirement,
) -> bool {
    slots
        .into_iter()
        .any(|slot| slot.admits(requirement).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: &[u8]) -> ContentHash {
        ContentHash::digest(seed)
    }

    fn slot(tenant_id: TenantId, kind: BackendKind) -> SlotCapability {
        SlotCapability {
            tenant_id,
            worker_id: WorkerId::new(),
            pool_id: DevicePoolId::new(),
            slot_id: SlotId::new(),
            backend_kind: kind,
            backend_version: "b1".to_owned(),
            model_versions: BTreeSet::new(),
            custom_nodes: BTreeMap::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            incapable_versions: BTreeSet::new(),
        }
    }

    #[test]
    fn rejects_other_tenants() {
        let tenant = TenantId::new();
        let slot = slot(TenantId::new(), BackendKind::LlamaCpp);
        let requirement = Requirement::for_model(tenant, hash(b"m"), None);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::TenantMismatch)
        );
    }

    #[test]
    fn rejects_wrong_runtime_kind() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::ComfyUi);
        slot.model_versions.insert(hash(b"m"));
        let requirement = Requirement::for_model(tenant, hash(b"m"), None);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::BackendMismatch {
                required: BackendKind::LlamaCpp,
                found: BackendKind::ComfyUi
            })
        );
    }

    #[test]
    fn requires_exact_model_version() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::LlamaCpp);
        slot.model_versions.insert(hash(b"other"));
        let requirement = Requirement::for_model(tenant, hash(b"m"), None);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::MissingModel(hash(b"m")))
        );
        slot.model_versions.insert(hash(b"m"));
        assert_eq!(slot.admits(&requirement), Ok(()));
    }

    #[test]
    fn requires_exact_custom_node_versions() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::ComfyUi);
        slot.model_versions.insert(hash(b"m"));
        slot.custom_nodes
            .insert("comfyui-extra".to_owned(), "1.2.0".to_owned());
        let manifest = WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "images".to_owned(),
            artifact_kind: crate::artifact::MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![hash(b"m")],
            required_custom_nodes: BTreeMap::from([(
                "comfyui-extra".to_owned(),
                "1.3.0".to_owned(),
            )]),
        };
        let requirement = Requirement::for_workflow(tenant, hash(b"w"), &manifest, None);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::MissingCustomNode {
                name: "comfyui-extra".to_owned(),
                required: "1.3.0".to_owned(),
                found: Some("1.2.0".to_owned()),
            })
        );
    }

    #[test]
    fn extra_models_and_custom_nodes_do_not_block_admission() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::ComfyUi);
        slot.model_versions.insert(hash(b"m"));
        slot.model_versions.insert(hash(b"unrelated"));
        slot.custom_nodes
            .insert("comfyui-extra".to_owned(), "1.3.0".to_owned());
        slot.custom_nodes
            .insert("comfyui-unused".to_owned(), "9.9.9".to_owned());
        let manifest = WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "images".to_owned(),
            artifact_kind: crate::artifact::MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![hash(b"m")],
            required_custom_nodes: BTreeMap::from([(
                "comfyui-extra".to_owned(),
                "1.3.0".to_owned(),
            )]),
        };
        let requirement = Requirement::for_workflow(tenant, hash(b"w"), &manifest, None);
        assert_eq!(slot.admits(&requirement), Ok(()));
    }

    #[test]
    fn missing_custom_node_reports_no_installed_version() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::ComfyUi);
        slot.model_versions.insert(hash(b"m"));
        let manifest = WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "images".to_owned(),
            artifact_kind: crate::artifact::MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![hash(b"m")],
            required_custom_nodes: BTreeMap::from([(
                "comfyui-extra".to_owned(),
                "1.3.0".to_owned(),
            )]),
        };
        let requirement = Requirement::for_workflow(tenant, hash(b"w"), &manifest, None);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::MissingCustomNode {
                name: "comfyui-extra".to_owned(),
                required: "1.3.0".to_owned(),
                found: None,
            })
        );
    }

    #[test]
    fn unknown_memory_does_not_reject_work() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::LlamaCpp);
        slot.model_versions.insert(hash(b"m"));
        let requirement = Requirement::for_model(tenant, hash(b"m"), Some(48 * 1024 * 1024 * 1024));
        assert_eq!(slot.admits(&requirement), Ok(()));

        slot.accelerator_memory_bytes = Some(8 * 1024 * 1024 * 1024);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::InsufficientMemory {
                required: 48 * 1024 * 1024 * 1024,
                available: 8 * 1024 * 1024 * 1024,
            })
        );
    }

    #[test]
    fn accelerator_memory_boundary_is_inclusive() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::LlamaCpp);
        slot.model_versions.insert(hash(b"m"));
        slot.accelerator_memory_bytes = Some(8 * 1024 * 1024 * 1024);

        let exact = Requirement::for_model(tenant, hash(b"m"), Some(8 * 1024 * 1024 * 1024));
        assert_eq!(slot.admits(&exact), Ok(()));

        let over = Requirement::for_model(tenant, hash(b"m"), Some(8 * 1024 * 1024 * 1024 + 1));
        assert_eq!(
            slot.admits(&over),
            Err(IncapableReason::InsufficientMemory {
                required: 8 * 1024 * 1024 * 1024 + 1,
                available: 8 * 1024 * 1024 * 1024,
            })
        );
    }

    #[test]
    fn oom_marking_removes_the_slot_from_candidates() {
        let tenant = TenantId::new();
        let mut slot = slot(tenant, BackendKind::LlamaCpp);
        slot.model_versions.insert(hash(b"m"));
        let requirement = Requirement::for_model(tenant, hash(b"m"), None);
        assert!(any_candidate_remains([&slot], &requirement));

        slot.mark_incapable(hash(b"m"));
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::KnownIncapable(hash(b"m")))
        );
        assert!(!any_candidate_remains([&slot], &requirement));
    }

    #[test]
    fn resident_model_is_recognized_for_both_backends() {
        let tenant = TenantId::new();
        let mut llama = slot(tenant, BackendKind::LlamaCpp);
        llama.model_versions.insert(hash(b"m"));
        llama.resident_model = Some(hash(b"m"));
        let model_requirement = Requirement::for_model(tenant, hash(b"m"), None);
        assert!(llama.holds_resident_model(&model_requirement));

        let manifest = WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "images".to_owned(),
            artifact_kind: crate::artifact::MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![hash(b"sd")],
            required_custom_nodes: BTreeMap::new(),
        };
        let mut comfy = slot(tenant, BackendKind::ComfyUi);
        comfy.model_versions.insert(hash(b"sd"));
        comfy.resident_model = Some(hash(b"sd"));
        let workflow_requirement = Requirement::for_workflow(tenant, hash(b"w"), &manifest, None);
        assert!(comfy.holds_resident_model(&workflow_requirement));
    }
}
