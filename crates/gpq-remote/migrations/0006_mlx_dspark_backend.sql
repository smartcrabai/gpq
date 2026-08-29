-- Allow Workers to advertise managed mlx-dspark LLM runtimes.
ALTER TABLE device_pools
    DROP CONSTRAINT device_pools_backend_kind_check,
    ADD CONSTRAINT device_pools_backend_kind_check
    CHECK (backend_kind IN ('llama_cpp', 'mlx_dspark', 'comfyui'));
