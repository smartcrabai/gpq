//! Compiles the Protobuf Edition 2024 schemas into `OUT_DIR`.
//!
//! ADR 0004: schemas declare `edition = "2024"` and compile through
//! `connectrpc-build` and Buffa using `protoc` 33 or newer. Buf is used in CI
//! for lint and breaking-change comparison only, never for code generation.

use std::path::Path;

const PROTOS: &[&str] = &[
    "proto/gpq/v1/common.proto",
    "proto/gpq/v1/generation.proto",
    "proto/gpq/v1/catalog.proto",
    "proto/gpq/v1/tenant.proto",
    "proto/gpq/worker/v1/worker.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Schemas live at the workspace root so Buf can lint one tree.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or("crate is not inside the workspace")?
        .to_path_buf();
    let files: Vec<_> = PROTOS.iter().map(|proto| root.join(proto)).collect();
    let includes = [root.join("proto")];

    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }

    connectrpc_build::Config::new()
        .files(&files)
        .includes(&includes)
        .include_file("_gpq_proto.rs")
        .compile()?;

    Ok(())
}
