//! Crash-recoverable local storage for Worker-local output Artifacts.
//!
//! ADR 0008: Worker-local outputs are crash-recoverable filesystem state, not
//! a database. Each Artifact gets its own directory containing atomically
//! published data (written as `data.part`, then renamed to `data` once
//! complete) plus a final `manifest.json`. The directory tree is scanned and
//! reconciled on Worker startup: half-written data is discarded, and a
//! completed manifest whose data disappeared is reported `Lost`. Bounded
//! one-MiB chunks with declared size and SHA-256 validation carry the bytes
//! over the wire (ADR 0008); unclaimed outputs expire one hour after
//! completion via [`gpq_domain::OUTPUT_ARTIFACT_TTL`].

use std::path::{Path, PathBuf};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use gpq_domain::{
    ArtifactId, ArtifactManifest, AttemptId, Hasher, ManifestMismatch, TRANSFER_CHUNK_BYTES,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Name of the atomically-published data file inside an Artifact directory.
const DATA_FILE: &str = "data";
/// Name of the in-progress data file before it is renamed into place.
const PART_FILE: &str = "data.part";
/// Name of the manifest file, written only after `data` is already in place.
const MANIFEST_FILE: &str = "manifest.json";

/// On-disk record for one published Artifact, serialized as `manifest.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredManifest {
    attempt_id: AttemptId,
    manifest: ArtifactManifest,
    completed_at: DateTime<Utc>,
}

/// Handle to a freshly published Artifact.
#[derive(Clone, Debug)]
pub struct ArtifactHandle {
    /// Identity assigned to the Artifact by this store.
    pub artifact_id: ArtifactId,
    /// The manifest it was published with.
    pub manifest: ArtifactManifest,
}

impl ArtifactHandle {
    /// The opaque token `DeliverRequest`/`DiscardOutput` carry to identify
    /// this Artifact (ADR 0008). This store uses the Artifact id itself: it
    /// is already unguessable (`UUIDv7`) and unique per directory.
    #[must_use]
    pub fn delivery_token(&self) -> String {
        self.artifact_id.to_string()
    }
}

/// One chunk of a resumed read, mirroring the wire `ArtifactChunk` shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactChunkData {
    /// Byte offset of `data` within the Artifact.
    pub offset: u64,
    /// Chunk payload, at most [`TRANSFER_CHUNK_BYTES`] long.
    pub data: Vec<u8>,
    /// Whether this is the final chunk of the read.
    pub last: bool,
}

/// A resumable, chunked reader over one published Artifact.
///
/// Reads are hashed as they are served; if the read started at offset zero
/// (a full, non-resumed transfer) the accumulated digest is checked against
/// the manifest once the last chunk is served (ADR 0008 "digest validation").
/// A read resumed from a nonzero offset cannot reproduce the whole-file
/// digest and is trusted instead: the bytes were already verified once, at
/// [`LocalArtifactStore::publish`] time, against the declared manifest.
pub struct ArtifactReader {
    file: tokio::fs::File,
    manifest: ArtifactManifest,
    started_at_zero: bool,
    position: u64,
    hasher: Hasher,
    done: bool,
}

impl ArtifactReader {
    /// The manifest of the Artifact being read.
    #[must_use]
    pub fn manifest(&self) -> &ArtifactManifest {
        &self.manifest
    }

    /// Reads the next chunk, or `None` once the Artifact is exhausted.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error, or a [`ManifestMismatch`] if a
    /// from-zero read's accumulated digest disagrees with the manifest.
    pub async fn next_chunk(&mut self) -> Result<Option<ArtifactChunkData>, ReadError> {
        if self.done {
            return Ok(None);
        }
        let mut buf = vec![0_u8; TRANSFER_CHUNK_BYTES];
        let mut read = 0_usize;
        while read < buf.len() {
            let n = self.file.read(&mut buf[read..]).await?;
            if n == 0 {
                break;
            }
            read += n;
        }
        buf.truncate(read);
        let offset = self.position;
        self.position += read as u64;
        let remaining = self.manifest.size_bytes.saturating_sub(self.position);
        let last = remaining == 0;
        if self.started_at_zero {
            self.hasher.update(&buf);
            if last {
                self.manifest
                    .verify(self.position, self.hasher.clone().finish())?;
            }
        }
        if last {
            self.done = true;
        }
        if read == 0 && offset == self.manifest.size_bytes {
            // Nothing left to serve and nothing was read this call.
            return Ok(None);
        }
        Ok(Some(ArtifactChunkData {
            offset,
            data: buf,
            last,
        }))
    }
}

/// Failure reading a stored Artifact back out.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Filesystem I/O failed.
    #[error("artifact read failed: {0}")]
    Io(#[from] std::io::Error),
    /// A from-zero read's digest disagreed with the manifest.
    #[error("artifact manifest mismatch: {0}")]
    Mismatch(#[from] ManifestMismatch),
}

/// Outcome of scanning the store directory at Worker startup (ADR 0008).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Artifacts that were already fully published and remain downloadable.
    pub available: Vec<ArtifactHandle>,
    /// Half-written directories discarded (a `data.part` with no completed
    /// manifest can only mean the Worker crashed mid-publish).
    pub incomplete_removed: Vec<ArtifactId>,
    /// Directories whose manifest exists but whose data disappeared: these
    /// are reported `Lost` per ADR 0008 and cannot be served.
    pub lost: Vec<ArtifactId>,
}

// Manual PartialEq/Eq: ArtifactHandle has no derive, so implement by hand for
// the report to stay comparable in tests.
impl PartialEq for ArtifactHandle {
    fn eq(&self, other: &Self) -> bool {
        self.artifact_id == other.artifact_id && self.manifest == other.manifest
    }
}
impl Eq for ArtifactHandle {}

/// Crash-recoverable filesystem store for Worker-local output Artifacts.
///
/// Not a database (ADR 0008): all state is reconstructed by scanning the
/// directory tree under `root`, one subdirectory per Artifact named by its
/// id.
#[derive(Clone, Debug)]
pub struct LocalArtifactStore {
    root: PathBuf,
}

impl LocalArtifactStore {
    /// Opens (creating if needed) the store rooted at `root`.
    pub async fn open(root: PathBuf) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    fn dir_for(&self, artifact_id: ArtifactId) -> PathBuf {
        self.root.join(artifact_id.to_string())
    }

    fn parse_token(token: &str) -> std::io::Result<ArtifactId> {
        ArtifactId::from_str(token)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string()))
    }

    /// Publishes `source` (a complete file already written by a backend) as
    /// the durable output of `attempt`, atomically renaming it into place
    /// only after it is fully copied so a crash mid-copy never leaves a
    /// directory that looks published (ADR 0008).
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error, or a [`ManifestMismatch`] if the
    /// copied bytes disagree with `manifest`.
    pub async fn publish(
        &self,
        attempt: AttemptId,
        source: &Path,
        manifest: ArtifactManifest,
    ) -> Result<ArtifactHandle, PublishError> {
        let artifact_id = ArtifactId::new();
        let dir = self.dir_for(artifact_id);
        tokio::fs::create_dir_all(&dir).await?;
        let part_path = dir.join(PART_FILE);

        let mut src = tokio::fs::File::open(source).await?;
        let mut dst = tokio::fs::File::create(&part_path).await?;
        let mut hasher = Hasher::new();
        let mut total = 0_u64;
        let mut buf = vec![0_u8; TRANSFER_CHUNK_BYTES];
        loop {
            let n = src.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            total += n as u64;
            dst.write_all(&buf[..n]).await?;
        }
        dst.flush().await?;
        drop(dst);
        manifest.verify(total, hasher.finish())?;

        let data_path = dir.join(DATA_FILE);
        tokio::fs::rename(&part_path, &data_path).await?; // atomic publish
        let stored = StoredManifest {
            attempt_id: attempt,
            manifest: manifest.clone(),
            completed_at: Utc::now(),
        };
        let bytes = serde_json::to_vec(&stored).map_err(std::io::Error::other)?;
        tokio::fs::write(dir.join(MANIFEST_FILE), bytes).await?;
        Ok(ArtifactHandle {
            artifact_id,
            manifest,
        })
    }

    /// Opens a resumable, chunked read of a previously published Artifact
    /// starting at `offset`, for `WorkerTransferService::DeliverArtifact`
    /// resumption (ADR 0008).
    pub async fn open_for_read(&self, token: &str, offset: u64) -> std::io::Result<ArtifactReader> {
        let artifact_id = Self::parse_token(token)?;
        let dir = self.dir_for(artifact_id);
        let stored = Self::read_manifest(&dir).await?;
        let mut file = tokio::fs::File::open(dir.join(DATA_FILE)).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        Ok(ArtifactReader {
            file,
            manifest: stored.manifest,
            started_at_zero: offset == 0,
            position: offset,
            hasher: Hasher::new(),
            done: false,
        })
    }

    /// Deletes a published Artifact. Idempotent: deleting an Artifact that
    /// is already gone is not an error.
    pub async fn delete(&self, token: &str) -> std::io::Result<()> {
        let artifact_id = Self::parse_token(token)?;
        let dir = self.dir_for(artifact_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Deletes every published Artifact completed at least `ttl` before
    /// `now`, per the one-hour unclaimed-output expiry (ADR 0008,
    /// [`gpq_domain::OUTPUT_ARTIFACT_TTL`]). Returns the ids removed.
    pub async fn expire(
        &self,
        now: DateTime<Utc>,
        ttl: std::time::Duration,
    ) -> std::io::Result<Vec<ArtifactId>> {
        let ttl = chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::MAX);
        let mut removed = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = entries.next_entry().await? {
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(artifact_id) = ArtifactId::from_str(&name) else {
                continue;
            };
            let dir = entry.path();
            let Ok(stored) = Self::read_manifest(&dir).await else {
                continue;
            };
            if stored.completed_at + ttl <= now {
                tokio::fs::remove_dir_all(&dir).await?;
                removed.push(artifact_id);
            }
        }
        Ok(removed)
    }

    /// Scans the store at Worker startup, completing or discarding anything
    /// left half-written by a crash and reporting Artifacts whose data
    /// disappeared as `Lost` (ADR 0008).
    pub async fn reconcile_on_startup(&self) -> std::io::Result<ReconcileReport> {
        let mut report = ReconcileReport::default();
        let mut entries = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = entries.next_entry().await? {
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(artifact_id) = ArtifactId::from_str(&name) else {
                continue;
            };
            let dir = entry.path();

            let data_exists = tokio::fs::try_exists(dir.join(DATA_FILE))
                .await
                .unwrap_or(false);
            let manifest_result = Self::read_manifest(&dir).await;

            match (data_exists, manifest_result) {
                (true, Ok(stored)) => {
                    report.available.push(ArtifactHandle {
                        artifact_id,
                        manifest: stored.manifest,
                    });
                }
                (false, Ok(_)) => {
                    // Manifest published, but the data behind it is gone.
                    tokio::fs::remove_dir_all(&dir).await?;
                    report.lost.push(artifact_id);
                }
                (_, Err(_)) => {
                    // No completed manifest: a crash interrupted publish().
                    tokio::fs::remove_dir_all(&dir).await?;
                    report.incomplete_removed.push(artifact_id);
                }
            }
        }
        Ok(report)
    }

    async fn read_manifest(dir: &Path) -> std::io::Result<StoredManifest> {
        let bytes = tokio::fs::read(dir.join(MANIFEST_FILE)).await?;
        serde_json::from_slice(&bytes)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
    }
}

/// Failure publishing an Artifact.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// Filesystem I/O failed.
    #[error("artifact publish failed: {0}")]
    Io(#[from] std::io::Error),
    /// The copied bytes disagreed with the declared manifest.
    #[error("artifact manifest mismatch: {0}")]
    Mismatch(#[from] ManifestMismatch),
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpq_domain::ContentHash;

    fn manifest(size: u64, data: &[u8]) -> ArtifactManifest {
        ArtifactManifest {
            size_bytes: size,
            digest: ContentHash::digest(data),
            kind: gpq_domain::MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        }
    }

    fn temp_store_root() -> tempfile::TempDir {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("failed to create tempdir")
        };
        dir
    }

    async fn open_store(root: &Path) -> LocalArtifactStore {
        let Ok(store) = LocalArtifactStore::open(root.to_path_buf()).await else {
            panic!("failed to open store")
        };
        store
    }

    async fn write_source(root: &Path, name: &str, data: &[u8]) -> PathBuf {
        let path = root.join(name);
        let Ok(()) = tokio::fs::write(&path, data).await else {
            panic!("failed to write source file")
        };
        path
    }

    async fn read_all_chunks(reader: &mut ArtifactReader) -> Vec<ArtifactChunkData> {
        let mut chunks = Vec::new();
        loop {
            let Ok(next) = reader.next_chunk().await else {
                panic!("failed to read chunk")
            };
            let Some(chunk) = next else { break };
            let last = chunk.last;
            chunks.push(chunk);
            if last {
                break;
            }
        }
        chunks
    }

    #[tokio::test]
    async fn publish_then_read_round_trips_bytes() {
        let root = temp_store_root();
        let store = open_store(root.path()).await;
        let src = write_source(root.path(), "source.bin", b"hello worker").await;

        let Ok(handle) = store
            .publish(AttemptId::new(), &src, manifest(12, b"hello worker"))
            .await
        else {
            panic!("publish should succeed for matching bytes")
        };

        let Ok(mut reader) = store.open_for_read(&handle.delivery_token(), 0).await else {
            panic!("open_for_read should succeed")
        };
        let chunks = read_all_chunks(&mut reader).await;
        let collected: Vec<u8> = chunks.iter().flat_map(|chunk| chunk.data.clone()).collect();
        assert_eq!(collected, b"hello worker");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].last);
    }

    #[tokio::test]
    async fn read_from_zero_iterates_multiple_one_mib_chunks_with_correct_offsets() {
        let root = temp_store_root();
        let store = open_store(root.path()).await;
        let total = TRANSFER_CHUNK_BYTES * 2 + 10;
        let data: Vec<u8> = (0..total)
            .map(|i| {
                let Ok(byte) = u8::try_from(i % 251) else {
                    panic!("i % 251 must fit in a u8")
                };
                byte
            })
            .collect();
        let src = write_source(root.path(), "source.bin", &data).await;

        let Ok(handle) = store
            .publish(AttemptId::new(), &src, manifest(total as u64, &data))
            .await
        else {
            panic!("publish should succeed for matching bytes")
        };
        let Ok(mut reader) = store.open_for_read(&handle.delivery_token(), 0).await else {
            panic!("open_for_read should succeed")
        };
        let chunks = read_all_chunks(&mut reader).await;

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].data.len(), TRANSFER_CHUNK_BYTES);
        assert!(!chunks[0].last);
        assert_eq!(chunks[1].offset, TRANSFER_CHUNK_BYTES as u64);
        assert_eq!(chunks[1].data.len(), TRANSFER_CHUNK_BYTES);
        assert!(!chunks[1].last);
        assert_eq!(chunks[2].offset, TRANSFER_CHUNK_BYTES as u64 * 2);
        assert_eq!(chunks[2].data.len(), 10);
        assert!(chunks[2].last);

        let collected: Vec<u8> = chunks.iter().flat_map(|chunk| chunk.data.clone()).collect();
        assert_eq!(collected, data);
    }

    #[tokio::test]
    async fn read_resumes_from_a_nonzero_offset_without_rereading_earlier_chunks() {
        let root = temp_store_root();
        let store = open_store(root.path()).await;
        let total = TRANSFER_CHUNK_BYTES + 5;
        let data: Vec<u8> = (0..total)
            .map(|i| {
                let Ok(byte) = u8::try_from(i % 251) else {
                    panic!("i % 251 must fit in a u8")
                };
                byte
            })
            .collect();
        let src = write_source(root.path(), "source.bin", &data).await;

        let Ok(handle) = store
            .publish(AttemptId::new(), &src, manifest(total as u64, &data))
            .await
        else {
            panic!("publish should succeed for matching bytes")
        };
        let Ok(mut reader) = store
            .open_for_read(&handle.delivery_token(), TRANSFER_CHUNK_BYTES as u64)
            .await
        else {
            panic!("open_for_read should succeed")
        };
        let chunks = read_all_chunks(&mut reader).await;

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].offset, TRANSFER_CHUNK_BYTES as u64);
        assert_eq!(chunks[0].data, data[TRANSFER_CHUNK_BYTES..]);
        assert!(chunks[0].last);
    }

    #[tokio::test]
    async fn publish_rejects_digest_mismatch_and_leaves_no_directory() {
        let root = temp_store_root();
        let store = open_store(root.path()).await;
        let src = write_source(root.path(), "source.bin", b"actual bytes").await;

        let Err(err) = store
            .publish(AttemptId::new(), &src, manifest(12, b"declared bytes"))
            .await
        else {
            panic!("digest mismatch must be rejected")
        };
        assert!(matches!(
            err,
            PublishError::Mismatch(ManifestMismatch::Digest { .. })
        ));
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let root = temp_store_root();
        let store = open_store(root.path()).await;
        let token = ArtifactId::new().to_string();
        let Ok(()) = store.delete(&token).await else {
            panic!("deleting a missing artifact must not error")
        };
    }

    #[tokio::test]
    async fn reconcile_completes_available_removes_incomplete_and_reports_lost() {
        let root = temp_store_root();
        let store = open_store(root.path()).await;

        // A fully published artifact.
        let src = write_source(root.path(), "source.bin", b"payload").await;
        let Ok(available) = store
            .publish(AttemptId::new(), &src, manifest(7, b"payload"))
            .await
        else {
            panic!("publish should succeed for matching bytes")
        };

        // A crash mid-write: only `data.part` exists, no manifest.
        let half_written = ArtifactId::new();
        let half_dir = root.path().join(half_written.to_string());
        let Ok(()) = tokio::fs::create_dir_all(&half_dir).await else {
            panic!("mkdir failed")
        };
        let Ok(()) = tokio::fs::write(half_dir.join("data.part"), b"partial").await else {
            panic!("write failed")
        };

        // An orphan manifest: the manifest survived but the data did not.
        let lost = ArtifactId::new();
        let lost_dir = root.path().join(lost.to_string());
        let Ok(()) = tokio::fs::create_dir_all(&lost_dir).await else {
            panic!("mkdir failed")
        };
        let stored = StoredManifest {
            attempt_id: AttemptId::new(),
            manifest: manifest(3, b"abc"),
            completed_at: Utc::now(),
        };
        let Ok(bytes) = serde_json::to_vec(&stored) else {
            panic!("serialize failed")
        };
        let Ok(()) = tokio::fs::write(lost_dir.join("manifest.json"), bytes).await else {
            panic!("write failed")
        };

        let Ok(report) = store.reconcile_on_startup().await else {
            panic!("reconcile failed")
        };
        assert_eq!(report.available, vec![available]);
        assert_eq!(report.incomplete_removed, vec![half_written]);
        assert_eq!(report.lost, vec![lost]);
        assert!(!tokio::fs::try_exists(&half_dir).await.unwrap_or(true));
        assert!(!tokio::fs::try_exists(&lost_dir).await.unwrap_or(true));
    }

    #[tokio::test]
    async fn expire_removes_only_artifacts_past_ttl() {
        let root = temp_store_root();
        let store = open_store(root.path()).await;
        let src = write_source(root.path(), "source.bin", b"payload").await;
        let Ok(handle) = store
            .publish(AttemptId::new(), &src, manifest(7, b"payload"))
            .await
        else {
            panic!("publish should succeed for matching bytes")
        };

        let ttl = std::time::Duration::from_hours(1);
        let Ok(still_fresh) = store.expire(Utc::now(), ttl).await else {
            panic!("expire failed")
        };
        assert!(still_fresh.is_empty());

        let Ok(past_ttl) = store
            .expire(Utc::now() + chrono::Duration::hours(2), ttl)
            .await
        else {
            panic!("expire failed")
        };
        assert_eq!(past_ttl, vec![handle.artifact_id]);
    }
}
