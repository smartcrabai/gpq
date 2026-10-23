# Let each Worker own native inference pools

One Worker per host owns non-overlapping Device Pools. Each Pool selects a native device such as `metal`, `vulkan0`, `cuda0`, or `cpu` and runs one private inference child at a time. The child is the hidden `infer` role of the same `gpq` executable; it runs native inference through the embedded bridge rather than launching an operator-installed model server, Python runtime, or other inference executable. The Pool serializes one Generation at a time.

Worker TOML defines the Pool's device selector, private loopback listener, state directory, startup timeout, native runtime options, shape limits, model asset paths, and LoRAs. Configuration loads at startup. It does not configure arbitrary executable paths, external server URLs, shell commands, or process argument vectors. Operators provision the required external model assets and drivers; the Worker does not download models.

The Worker hashes required family-specific asset roles, default LoRA assets/scales, and the sampling preset into a canonical immutable Model Manifest. Configured `expected_hashes` reject substituted or modified model files; additional image LoRAs are separately hashed and advertised. Scheduling requires the exact manifest version, requested LoRAs, modality, and configured shape bounds. Optional memory estimates are observations, not admission gates.

The Worker starts and supervises the child under the existing Unix process-group or Windows Job Object ownership model. Its authenticated inference control endpoint is private to loopback. Cancellation, OOM, or hung native code terminates the managed child before the Pool is reused; process termination remains the portable recovery fallback. Do not adopt or kill an unrelated process by PID alone.
