# Use six Rust crates

The workspace contains six crates: `gpq`, `gpq-domain`, `gpq-inference`, `gpq-proto`, `gpq-remote`, and `gpq-worker`. `gpq` is the sole application binary and dispatches `remote`, `worker`, and private `infer` roles; the remaining crates provide focused domain, native inference, protocol, Remote, and Worker boundaries. Separate storage, SDK, and configuration crates remain deferred until a distinct consumer requires them.
