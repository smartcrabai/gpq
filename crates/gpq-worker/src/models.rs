//! Model Version content hashing and caching (ADR 0005, ADR 0012).
//!
//! Workers compute SHA-256 Model Version hashes and cache them atomically by
//! path, size, modification time, and file identity, rehashing on any change
//! and rejecting configured `expected_hashes` mismatches rather than silently
//! trusting stale or substituted model material.

use std::collections::BTreeMap;
use std::fs::Metadata;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::Context;
use gpq_domain::ContentHash;
use gpq_domain::hash::Hasher;
use serde::{Deserialize, Serialize};

use crate::config::PoolConfig;

/// Streamed read chunk size while hashing model files.
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    key: CacheKey,
    hash: ContentHash,
}

/// Path -> cache entry, persisted as one JSON file per Pool state directory.
type CacheFile = BTreeMap<String, CacheEntry>;

/// Computes (or returns the cached) SHA-256 [`ContentHash`] of the file at
/// `path`.
///
/// The cache lives under `state_dir` as `model-hash-cache.json`, keyed by
/// path, file size, modification time, and platform file identity; any
/// mismatch on any of those forces a fresh hash, and the cache file itself is
/// written atomically (temp file + rename) so a crash mid-write never leaves
/// a corrupt cache (ADR 0005).
pub fn hash_model(state_dir: &Path, path: &Path) -> anyhow::Result<ContentHash> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading metadata for model {}", path.display()))?;
    let key = cache_key(&metadata)?;
    let path_key = path.to_string_lossy().into_owned();
    let cache_path = state_dir.join(CACHE_FILE_NAME);

    let mut cache = load_cache(&cache_path)?;
    if let Some(entry) = cache.get(&path_key)
        && entry.key == key
    {
        return Ok(entry.hash);
    }

    let hash = digest_file(path)?;
    cache.insert(path_key, CacheEntry { key, hash });
    save_cache(&cache_path, &cache)?;
    Ok(hash)
}

/// Hashes every model configured for `pool`, rejecting (with an `Err`) any
/// computed hash that disagrees with `pool.expected_hashes` (ADR 0005: a
/// mismatch is rejected rather than silently trusted).
pub fn scan_models(pool: &PoolConfig) -> anyhow::Result<Vec<(PathBuf, ContentHash)>> {
    pool.model_paths
        .iter()
        .map(|path| {
            let hash = hash_model(&pool.state_dir, path)?;
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

fn digest_file(path: &Path) -> anyhow::Result<ContentHash> {
    let mut file =
        std::fs::File::open(path).with_context(|| format!("opening model {}", path.display()))?;
    let mut hasher = Hasher::new();
    let mut buffer = vec![0_u8; HASH_CHUNK_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("reading model {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finish())
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

    use super::hash_model;

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
