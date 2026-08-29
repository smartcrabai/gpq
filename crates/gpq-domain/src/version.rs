//! Immutable Model and Workflow Versions.
//!
//! ADR 0012: aliases are mutable pointers, versions are not. ADR 0007 fixes the
//! minimal Workflow manifest Remote stores: output node and name, Artifact kind
//! and MIME type, required Model Versions and custom-node versions, estimated
//! VRAM, and execution timeout. ADR 0003 fixes how those timeouts compose.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::artifact::MediaKind;
use crate::hash::ContentHash;
use crate::id::{ModelVersionId, WorkflowVersionId};
use crate::modality::Modality;

/// Execution limits a version may declare.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionLimits {
    /// Replaces the modality default when present.
    pub execution_timeout: Option<Duration>,
    /// Accelerator memory the version is expected to need, in bytes.
    pub estimated_vram_bytes: Option<u64>,
}

/// Exact model material advertised by capable Workers (ADR 0012).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ModelVersion {
    /// Stored identity.
    pub id: ModelVersionId,
    /// Content hash of the model material, computed by the Worker (ADR 0005).
    pub content_hash: ContentHash,
    /// Modality the model serves.
    pub modality: Modality,
    /// Version-declared limits.
    pub limits: ExecutionLimits,
}

/// An immutable `ComfyUI` graph plus its output and execution manifest.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct WorkflowVersion {
    /// Stored identity.
    pub id: WorkflowVersionId,
    /// Content hash of the graph and manifest.
    pub content_hash: ContentHash,
    /// Modality the workflow serves.
    pub modality: Modality,
    /// Output and requirement manifest.
    pub manifest: WorkflowManifest,
    /// Version-declared limits.
    pub limits: ExecutionLimits,
}

/// The minimal output and execution contract of a Workflow Version (ADR 0007).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct WorkflowManifest {
    /// `ComfyUI` node id that produces the deliverable output.
    pub output_node: String,
    /// Output name within that node.
    pub output_name: String,
    /// Media kind of the produced Artifact.
    pub artifact_kind: MediaKind,
    /// MIME type of the produced Artifact.
    pub artifact_mime: String,
    /// Model Versions the graph loads; every one must be present on the Worker.
    pub required_models: Vec<ContentHash>,
    /// Custom-node package name to exact installed version.
    pub required_custom_nodes: BTreeMap<String, String>,
}

/// Resolves the execution timeout of an Attempt.
///
/// ADR 0003: a Model or Workflow Version may lower or replace the modality
/// default, and a Generation may only shorten the result. ADR 0006 lets a Tenant
/// cap every timeout, so the ceiling applies last and can only shorten further.
#[must_use]
pub fn resolve_execution_timeout(
    modality: Modality,
    version: ExecutionLimits,
    requested: Option<Duration>,
    tenant_ceiling: Duration,
) -> Duration {
    let base = version
        .execution_timeout
        .unwrap_or_else(|| modality.default_execution_timeout());
    let shortened = requested.map_or(base, |requested| requested.min(base));
    shortened.min(tenant_ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_hours(1);
    const DAY: Duration = Duration::from_hours(24);

    #[test]
    fn falls_back_to_the_modality_default() {
        assert_eq!(
            resolve_execution_timeout(Modality::Image, ExecutionLimits::default(), None, DAY),
            Duration::from_hours(2)
        );
    }

    #[test]
    fn version_limit_replaces_the_default() {
        let limits = ExecutionLimits {
            execution_timeout: Some(HOUR),
            estimated_vram_bytes: None,
        };
        assert_eq!(
            resolve_execution_timeout(Modality::Llm, limits, None, DAY),
            HOUR
        );
    }

    #[test]
    fn generation_may_only_shorten() {
        let limits = ExecutionLimits {
            execution_timeout: Some(HOUR),
            estimated_vram_bytes: None,
        };
        assert_eq!(
            resolve_execution_timeout(Modality::Llm, limits, Some(Duration::from_mins(1)), DAY),
            Duration::from_mins(1),
            "a shorter request wins"
        );
        assert_eq!(
            resolve_execution_timeout(Modality::Llm, limits, Some(DAY), DAY),
            HOUR,
            "a longer request is ignored"
        );
    }

    #[test]
    fn tenant_ceiling_caps_everything() {
        let limits = ExecutionLimits {
            execution_timeout: Some(DAY),
            estimated_vram_bytes: None,
        };
        assert_eq!(
            resolve_execution_timeout(Modality::Video, limits, None, HOUR),
            HOUR
        );
    }
}
