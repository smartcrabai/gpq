# Use four Rust crates

The workspace contains only `gpq-domain`, `gpq-proto`, `gpq-remote`, and `gpq-worker`. Remote uses Axum, connect-rust/Buffa, and SQLx; Worker uses connect-rust and Tokio process supervision, with llama.cpp, mlx-dspark, and ComfyUI adapters as internal modules. Separate storage, backend-interface, SDK, and configuration crates are deferred until more than one real consumer forces a boundary.
