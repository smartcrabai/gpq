//! Model Version content hashing and caching (ADR 0005, ADR 0012).
//!
//! Workers compute SHA-256 Model Version hashes and cache them atomically by
//! path and member metadata, rehashing on any change and rejecting configured
//! `expected_hashes` mismatches rather than silently
//! trusting stale or substituted model material.

use std::collections::BTreeMap;
use std::fs::Metadata;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::Context;
use gpq_domain::ContentHash;
use gpq_domain::hash::Hasher;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::config::PoolConfig;

/// Streamed read chunk size while hashing model material.
const HASH_CHUNK_BYTES: usize = 1024 * 1024;

/// Per-process counter mixed into temp cache file names so concurrent
/// `save_cache` calls (e.g. two Attempts hashing on the same Pool right
/// after startup, per ADR 0005) never race the same rename target.
static TEMP_FILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// File name of the hash cache persisted under a Pool's state directory.
const CACHE_FILE_NAME: &str = "model-hash-cache.json";

/// Everything that must stay identical for a cached hash to remain valid.
/// Any difference forces a rehash (ADR 0005).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CacheKey {
    size: u64,
    mtime_unix_nanos: i128,
    /// Platform file identity: Unix `dev:ino`, Windows `volume:file_index`.
    file_id: String,
    /// Distinguishes a model directory from a single model file.
    #[serde(default)]
    directory: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    key: CacheKey,
    /// Relative path to metadata for every entry in a model directory.
    #[serde(default)]
    members: BTreeMap<String, CacheKey>,
    hash: ContentHash,
}

/// Path -> cache entry, persisted as one JSON file per Pool state directory.
type CacheFile = BTreeMap<String, CacheEntry>;
type ModelFiles = (BTreeMap<String, CacheKey>, Vec<(String, PathBuf)>);

/// Computes (or returns the cached) SHA-256 [`ContentHash`] of one model.
///
/// Single-file models retain their raw file digest. Directory models use a
/// deterministic digest of every relative path and regular file, which lets
/// mlx-dspark models made of config, tokenizer, and sharded weight files obey
/// the same immutable-version contract.
pub fn hash_model(state_dir: &Path, path: &Path) -> anyhow::Result<ContentHash> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading metadata for model {}", path.display()))?;
    let key = cache_key(&metadata)?;
    let (members, files) = model_files(path, &metadata)?;
    let path_key = path.to_string_lossy().into_owned();
    let cache_path = state_dir.join(CACHE_FILE_NAME);

    let mut cache = load_cache(&cache_path)?;
    if let Some(entry) = cache.get(&path_key)
        && entry.key == key
        && entry.members == members
    {
        return Ok(entry.hash);
    }

    let hash = if key.directory {
        digest_directory(&files)?
    } else {
        digest_file(path)?
    };
    cache.insert(path_key, CacheEntry { key, members, hash });
    save_cache(&cache_path, &cache)?;
    Ok(hash)
}

/// Computes a model hash without consulting the metadata cache. This is used
/// at trust boundaries where a model's bytes must be checked, rather than
/// relying on metadata that a file replacement can preserve.
pub(crate) fn hash_model_fresh(path: &Path) -> anyhow::Result<ContentHash> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading metadata for model {}", path.display()))?;
    let (_, files) = model_files(path, &metadata)?;
    if metadata.is_dir() {
        digest_directory(&files)
    } else {
        digest_file(path)
    }
}

/// Hashes model material while checking `cancel` between directory entries and
/// bounded file reads. The blocking caller can therefore stop promptly when a
/// timed-out or cancelled attempt drops its awaiter.
pub(crate) fn hash_model_fresh_cancellable(
    path: &Path,
    cancel: &CancellationToken,
) -> anyhow::Result<ContentHash> {
    ensure_not_cancelled(cancel)?;
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading metadata for model {}", path.display()))?;
    let (_, files) = model_files_with_cancel(path, &metadata, cancel)?;
    if metadata.is_dir() {
        digest_directory_with_cancel(&files, cancel)
    } else {
        digest_file_with_cancel(path, cancel)
    }
}

/// Metadata fingerprint of one model file or directory: size, modification
/// time, and file identity of the model itself and of every member. Cheap
/// to read even for a multi-gigabyte model, so a backend adapter can bind a
/// content hash once per process and reuse it while the fingerprint is
/// unchanged, refusing to trust the material (rather than rehashing it) once
/// it differs (ADR 0012).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelSnapshot {
    key: CacheKey,
    members: BTreeMap<String, CacheKey>,
}

impl ModelSnapshot {
    pub(crate) fn read(path: &Path) -> anyhow::Result<Self> {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("reading metadata for model {}", path.display()))?;
        let key = cache_key(&metadata)?;
        let (members, _) = model_files(path, &metadata)?;
        Ok(Self { key, members })
    }
}

fn model_files(path: &Path, metadata: &Metadata) -> anyhow::Result<ModelFiles> {
    let mut members = BTreeMap::new();
    let mut files = Vec::new();
    if metadata.is_dir() {
        collect_model_files(path, path, &mut members, &mut files)?;
    } else if !metadata.is_file() {
        anyhow::bail!("model {} is not a file or directory", path.display());
    }
    Ok((members, files))
}

fn model_files_with_cancel(
    path: &Path,
    metadata: &Metadata,
    cancel: &CancellationToken,
) -> anyhow::Result<ModelFiles> {
    let mut members = BTreeMap::new();
    let mut files = Vec::new();
    if metadata.is_dir() {
        collect_model_files_with_cancel(path, path, &mut members, &mut files, cancel)?;
    } else if !metadata.is_file() {
        anyhow::bail!("model {} is not a file or directory", path.display());
    }
    Ok((members, files))
}

/// Hashes every model configured for `pool`, rejecting (with an `Err`) any
/// computed hash that disagrees with `pool.expected_hashes` (ADR 0005: a
/// mismatch is rejected rather than silently trusted).
pub fn scan_models(pool: &PoolConfig) -> anyhow::Result<Vec<(PathBuf, ContentHash)>> {
    pool.model_paths
        .iter()
        .map(|path| {
            let hash = hash_model_fresh(path)?;
            if let Some(expected_hex) = pool
                .expected_hashes
                .get(&path.to_string_lossy().into_owned())
            {
                let expected: ContentHash = expected_hex.parse().with_context(|| {
                    format!(
                        "pool `{}` expected hash for {} is malformed",
                        pool.key,
                        path.display()
                    )
                })?;
                anyhow::ensure!(
                    hash == expected,
                    "model {} hash mismatch: computed {} but pool `{}` expects {}",
                    path.display(),
                    hash.to_hex(),
                    pool.key,
                    expected_hex
                );
            }
            Ok((path.clone(), hash))
        })
        .collect()
}

fn cache_key(metadata: &Metadata) -> anyhow::Result<CacheKey> {
    let modified = metadata
        .modified()
        .context("reading file modification time")?;
    let mtime_unix_nanos = modified
        .duration_since(std::time::UNIX_EPOCH)
        .context("file modification time is before the Unix epoch")?
        .as_nanos()
        .try_into()
        .unwrap_or(i128::MAX);
    Ok(CacheKey {
        size: metadata.len(),
        mtime_unix_nanos,
        file_id: file_identity(metadata),
        directory: metadata.is_dir(),
    })
}

#[cfg(unix)]
fn file_identity(metadata: &Metadata) -> String {
    use std::os::unix::fs::MetadataExt;
    format!("{}:{}", metadata.dev(), metadata.ino())
}

#[cfg(windows)]
fn file_identity(metadata: &Metadata) -> String {
    use std::os::windows::fs::MetadataExt;
    // volume_serial_number()/file_index() are still unstable
    // (`windows_by_handle`, rust-lang/rust#63010). The creation time is the
    // best stable stand-in: it changes when the file is replaced, and the
    // cache key already includes size and mtime.
    format!("ctime:{}", metadata.creation_time())
}

/// Collects one model directory in stable relative-path order. Symlinked
/// files are hashed by content; symlinked directories are rejected to avoid
/// escaping the model tree or following cycles.
fn collect_model_files(
    root: &Path,
    directory: &Path,
    members: &mut BTreeMap<String, CacheKey>,
    files: &mut Vec<(String, PathBuf)>,
) -> anyhow::Result<()> {
    let no_cancel = CancellationToken::new();
    collect_model_files_with_cancel(root, directory, members, files, &no_cancel)
}

fn collect_model_files_with_cancel(
    root: &Path,
    directory: &Path,
    members: &mut BTreeMap<String, CacheKey>,
    files: &mut Vec<(String, PathBuf)>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    ensure_not_cancelled(cancel)?;
    let mut entries = std::fs::read_dir(directory)
        .with_context(|| format!("reading model directory {}", directory.display()))?
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        ensure_not_cancelled(cancel)?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("reading model entry {}", path.display()))?;
        let relative = relative_model_path(root, &path)?;
        members.insert(relative.clone(), cache_key(&metadata)?);
        if metadata.is_dir() {
            anyhow::ensure!(
                !file_type.is_symlink(),
                "model directory symlink {} is not supported",
                path.display()
            );
            collect_model_files_with_cancel(root, &path, members, files, cancel)?;
        } else if metadata.is_file() {
            files.push((relative, path));
        } else {
            anyhow::bail!("model entry {} is not a file or directory", path.display());
        }
    }
    Ok(())
}

fn relative_model_path(root: &Path, path: &Path) -> anyhow::Result<String> {
    path.strip_prefix(root)?
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .map(str::to_owned)
                .context("model paths must be valid UTF-8")
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|components| components.join("/"))
}

fn digest_directory(files: &[(String, PathBuf)]) -> anyhow::Result<ContentHash> {
    let no_cancel = CancellationToken::new();
    digest_directory_with_cancel(files, &no_cancel)
}

fn digest_directory_with_cancel(
    files: &[(String, PathBuf)],
    cancel: &CancellationToken,
) -> anyhow::Result<ContentHash> {
    let mut hasher = Hasher::new();
    hasher.update(b"gpq-model-directory-v1\0");
    let mut buffer = vec![0_u8; HASH_CHUNK_BYTES];
    for (relative, path) in files {
        ensure_not_cancelled(cancel)?;
        let name = relative.as_bytes();
        let name_len = u64::try_from(name.len()).context("model relative path is too long")?;
        let size = std::fs::metadata(path)?.len();
        hasher.update(&name_len.to_be_bytes());
        hasher.update(name);
        hasher.update(&size.to_be_bytes());
        update_file_hash_with_cancel(&mut hasher, path, &mut buffer, cancel)?;
    }
    Ok(hasher.finish())
}

fn digest_file(path: &Path) -> anyhow::Result<ContentHash> {
    let no_cancel = CancellationToken::new();
    digest_file_with_cancel(path, &no_cancel)
}

fn digest_file_with_cancel(path: &Path, cancel: &CancellationToken) -> anyhow::Result<ContentHash> {
    let mut hasher = Hasher::new();
    let mut buffer = vec![0_u8; HASH_CHUNK_BYTES];
    update_file_hash_with_cancel(&mut hasher, path, &mut buffer, cancel)?;
    Ok(hasher.finish())
}

fn update_file_hash_with_cancel(
    hasher: &mut Hasher,
    path: &Path,
    buffer: &mut [u8],
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("opening model {}", path.display()))?;
    loop {
        ensure_not_cancelled(cancel)?;
        let read = file
            .read(buffer)
            .with_context(|| format!("reading model {}", path.display()))?;
        if read == 0 {
            return Ok(());
        }
        hasher.update(&buffer[..read]);
    }
}

fn ensure_not_cancelled(cancel: &CancellationToken) -> anyhow::Result<()> {
    anyhow::ensure!(!cancel.is_cancelled(), "model hashing cancelled");
    Ok(())
}

fn load_cache(path: &Path) -> anyhow::Result<CacheFile> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("parsing model hash cache"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(CacheFile::new()),
        Err(err) => Err(err).context("reading model hash cache"),
    }
}

fn save_cache(path: &Path, cache: &CacheFile) -> anyhow::Result<()> {
    let Some(parent) = path.parent() else {
        anyhow::bail!("cache path {} has no parent directory", path.display());
    };
    std::fs::create_dir_all(parent)?;
    let unique = TEMP_FILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temp_path = parent.join(format!(
        ".model-hash-cache.tmp-{}-{unique}-{:016x}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let result = (|| -> anyhow::Result<()> {
        let body = serde_json::to_vec_pretty(cache)?;
        std::fs::write(&temp_path, body)
            .with_context(|| format!("writing {}", temp_path.display()))?;
        std::fs::rename(&temp_path, path)
            .with_context(|| format!("renaming cache into {}", path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use gpq_domain::ContentHash;

    use super::{hash_model, hash_model_fresh, hash_model_fresh_cancellable};

    fn write_file(path: &std::path::Path, content: &[u8]) {
        let Ok(mut file) = std::fs::File::create(path) else {
            panic!("create model file")
        };
        let Ok(()) = file.write_all(content) else {
            panic!("write model file")
        };
    }

    #[test]
    fn cache_hits_for_an_unchanged_file() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let model_path = dir.path().join("model.gguf");
        write_file(&model_path, b"weights v1");

        let Ok(first) = hash_model(dir.path(), &model_path) else {
            panic!("first hash")
        };
        let Ok(second) = hash_model(dir.path(), &model_path) else {
            panic!("second hash")
        };

        assert_eq!(first, second);
        assert_eq!(first, ContentHash::digest(b"weights v1"));
        assert!(dir.path().join("model-hash-cache.json").exists());
    }

    #[test]
    fn cache_misses_when_content_and_size_change() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let model_path = dir.path().join("model.gguf");
        write_file(&model_path, b"weights v1");
        let Ok(first) = hash_model(dir.path(), &model_path) else {
            panic!("first hash")
        };

        // Content and size both change here, which alone would already miss
        // the cache; also force the mtime forward so the test exercises that
        // half of the cache key deterministically instead of only relying on
        // the size difference.
        write_file(&model_path, b"weights v2, now longer");
        let Some(future) =
            std::time::SystemTime::now().checked_add(std::time::Duration::from_secs(5))
        else {
            panic!("computing future mtime")
        };
        let Ok(file) = std::fs::File::open(&model_path) else {
            panic!("reopen model file")
        };
        let Ok(()) = file.set_modified(future) else {
            panic!("bump mtime")
        };

        let Ok(second) = hash_model(dir.path(), &model_path) else {
            panic!("second hash")
        };

        assert_ne!(first, second);
        assert_eq!(second, ContentHash::digest(b"weights v2, now longer"));
    }

    #[test]
    fn fresh_hash_detects_same_metadata_content_replacement() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let model_path = dir.path().join("model.gguf");
        write_file(&model_path, b"weights v1");
        let Ok(modified) = std::fs::metadata(&model_path).and_then(|m| m.modified()) else {
            panic!("read mtime")
        };
        let Ok(first) = hash_model_fresh(&model_path) else {
            panic!("first fresh hash")
        };

        write_file(&model_path, b"weights v2");
        let Ok(file) = std::fs::File::open(&model_path) else {
            panic!("reopen model file")
        };
        let Ok(()) = file.set_modified(modified) else {
            panic!("restore mtime")
        };
        let Ok(second) = hash_model_fresh(&model_path) else {
            panic!("second fresh hash")
        };

        assert_ne!(first, second);
    }

    #[test]
    fn cancellable_hash_honors_pre_cancelled_token() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let model_path = dir.path().join("model.gguf");
        write_file(&model_path, b"weights");
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();

        assert!(hash_model_fresh_cancellable(&model_path, &cancel).is_err());
    }

    #[test]
    fn directory_hash_is_stable_and_tracks_member_changes() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let model_path = dir.path().join("model");
        let Ok(()) = std::fs::create_dir(&model_path) else {
            panic!("create model directory")
        };
        write_file(&model_path.join("config.json"), b"{}");
        write_file(&model_path.join("weights.safetensors"), b"weights v1");

        let Ok(first) = hash_model(dir.path(), &model_path) else {
            panic!("first directory hash")
        };
        let Ok(second) = hash_model(dir.path(), &model_path) else {
            panic!("cached directory hash")
        };
        assert_eq!(first, second);

        write_file(
            &model_path.join("weights.safetensors"),
            b"weights v2, longer",
        );
        let Ok(changed) = hash_model(dir.path(), &model_path) else {
            panic!("changed directory hash")
        };
        assert_ne!(first, changed);
    }

    #[test]
    fn scan_models_rejects_expected_hash_mismatch() {
        use std::collections::BTreeMap;

        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let model_path = dir.path().join("model.gguf");
        write_file(&model_path, b"weights v1");

        let mut expected_hashes = BTreeMap::new();
        expected_hashes.insert(model_path.to_string_lossy().into_owned(), "0".repeat(64));

        let Ok(base_url) = "http://127.0.0.1:8081/".parse() else {
            panic!("parse base url")
        };
        let pool = crate::config::PoolConfig {
            key: "pool-a".to_string(),
            backend: gpq_domain::BackendKind::LlamaCpp,
            executable: PathBufFixture::absolute(),
            args: Vec::new(),
            env: BTreeMap::new(),
            state_dir: dir.path().to_path_buf(),
            startup_timeout: std::time::Duration::from_secs(30),
            base_url,
            slots: None,
            model_paths: vec![model_path],
            expected_hashes,
        };

        let result = super::scan_models(&pool);

        assert!(result.is_err());
    }

    /// A platform-portable absolute path for tests that only need a
    /// syntactically valid `PoolConfig::executable`, never executed.
    struct PathBufFixture;

    impl PathBufFixture {
        #[cfg(unix)]
        fn absolute() -> std::path::PathBuf {
            std::path::PathBuf::from("/usr/bin/true")
        }

        #[cfg(windows)]
        fn absolute() -> std::path::PathBuf {
            std::path::PathBuf::from(r"C:\Windows\System32\cmd.exe")
        }
    }
}
