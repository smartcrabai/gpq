# gpq

GPU Generation Queue (`gpq`) coordinates tenant-owned GPU workers and typed image/video generation. One native executable serves the coordinator, worker, and private inference roles; model assets remain external.

## Architecture

- **`gpq remote`** serves the public API and uses PostgreSQL for durable queue, tenancy, lease, and artifact state.
- **`gpq worker`** runs on a GPU host, opens an outbound session to Remote, and supervises one native inference child per configured Device Pool.
- **`gpq infer`** is the private child role spawned by Worker; operators do not run it directly.
- A Worker belongs to one Tenant and never executes another Tenant's work. Generations use leased, at-least-once execution; each retry creates a distinct Attempt and the first committed success becomes the accepted result.
- One Remote instance is the supported deployment shape. PostgreSQL restart recovery, Worker reconnection, and lease expiry recover interrupted work.
- Remote-local input Artifact bytes are stored in PostgreSQL without requiring S3 and are deleted when their Generation terminates. Worker-local outputs are delivered once or expire; optional object-store placement remains available.

## Product scope

The public generation surface is native and typed. It supports Qwen Image 2.1 image generation/editing and MiniMax H3 FL2VA video generation with synchronized audio. There is no public LLM/chat, OpenAI-compatible, ComfyUI, workflow, music, or raw-prompt API.

Image requests use a model alias and typed `ImageParameters`: prompt, optional negative prompt, width/height, optional steps/CFG/sigmas, up to 10 reference Artifact IDs, and optional image LoRAs identified by SHA-256 and scale. Qwen Image 2.1 edits images natively through those references; no separate edit LoRA is required. Optional compatible style or Turbo LoRAs must be configured on an online Worker.

Video requests use typed `VideoParameters`: prompt, optional negative prompt, width/height, frame count, and optional first/last frame Artifact IDs (a last frame requires a first). Dimensions are positive multiples of 32, with a maximum video axis of 16,352 pixels imposed by VP8; H3 frame counts are at least 5 and follow `17k + 5`. Native video output is 24 fps WebM with VP8 video and synchronized Opus audio.

Default sampling is Qwen Image 2.1 at 40 steps / CFG 6, and MiniMax H3 at 4 steps / CFG 1 with sigmas `[1.0, 0.965517, 0.888889, 0.679245, 0.0]`. Configure default LoRAs and sampling overrides in each Worker's model manifest; request LoRAs are additional image LoRAs. Image/video execution timeout defaults are 2 hours / 24 hours. A request may only shorten its resolved timeout, and the Tenant execution-timeout ceiling applies. Worker shape limits bound image pixels, video total pixels, video frames, and image steps; they reject over-limit work rather than resizing it.

## API surfaces

All public surfaces share one Remote listener:

- Native Connect RPCs use `POST /<fully-qualified-service>/<method>` (for example, `POST /gpq.v1.GenerationService/Submit`), with JSON or binary Connect protocol. Public services authenticate with `Authorization: Bearer <Tenant Master Key>`:
  - `gpq.v1.GenerationService`: `Submit`, `GetGeneration`, `ListGenerations`, `CancelGeneration`, streaming `WatchGeneration`, `CreateInputArtifact`.
  - `gpq.v1.CatalogService`: `SetModelAlias`, `DeleteModelAlias`, `ListModels`, `ListWorkers`.
  - `gpq.v1.TenantService`: `GetTenantSettings`, `UpdateTenantSettings`.
- Worker gRPC services: `gpq.worker.v1.WorkerEnrollmentService/Enroll` is called by `gpq worker enroll`, which reads the Tenant Master Key from stdin; bidirectional `gpq.worker.v1.WorkerSessionService/Session` and `gpq.worker.v1.WorkerTransferService/FetchArtifact` / `gpq.worker.v1.WorkerTransferService/DeliverArtifact` use the enrolled Worker's revocable Worker Credential.
- Artifact transfer: `PUT /v1/artifacts/{artifact_id}` for Remote-local input upload and `GET /v1/artifacts/{artifact_id}` for Worker-local output download. `CreateInputArtifact` returns the upload URL; Remote-local input bytes use PostgreSQL `bytea` and do not require S3.
- Health checks: `GET /healthz` and `GET /readyz`; Tenant settings console: `/console`.

Generation submission requires an `idempotency-key` request header. A model alias resolves at admission to a content-pinned manifest version; alias changes do not alter queued or completed Generations. Public APIs use the Tenant Master Key; Worker protocol calls use only the enrolled Worker's tenant-scoped credential.

Remote serves plaintext HTTP/1.1 and h2c; terminate TLS at the ingress. The Worker connects to the Remote origin configured in its TOML file.

## Model assets

Models and supporting tokenizer/config, VAE, vision, and LoRA files are downloaded and licensed separately; they are not bundled in builds or fetched at runtime. Start with the [official Qwen Image 2.1 model card](https://huggingface.co/Qwen/Qwen-Image-2.1) and [Qwen project](https://github.com/QwenLM/Qwen-Image-2.1), the [official MiniMax H3 model card and license](https://huggingface.co/MiniMaxAI/MiniMax-H3), and [Kijai's experimental H3 model files](https://huggingface.co/Kijai/MiniMax-H3-experimental). The Q4 FL2VA quantization used by the example configuration is a [community conversion](https://huggingface.co/Abiray/MiniMax-H3-Pruned-GGUF/blob/main/MiniMax-H3-FL2VA-Pruned-Q4_K_M.gguf); the matched [FlashGen 4-step LoRA](https://huggingface.co/Beidouqixing/minimax-h3-4step-lora-flashgen) is a separate asset. Optional image style/Turbo LoRAs are deployment-supplied; image editing itself does not require one. Review and follow each source's current license and version requirements.

Workers hash model asset roles and default LoRAs together with the sampling preset into immutable manifests; extra image LoRAs are separately hashed and advertised. Pin downloaded files in deployment configuration with `expected_hashes` when reproducible asset identity is required; keep the SHA-256 values with deployment configuration rather than duplicating a large hash list in this README. The Worker validates configured expected hashes and advertises the resulting manifest hash for alias registration.

## Build and install

Build prerequisites:

- Rust 1.97 or newer, `protoc` 36.2, CMake (4.4.4 tested), and a C/C++ toolchain.
- Linux Vulkan builds need Vulkan development headers, `glslc`, and SPIRV-Headers. macOS builds use the macOS 27 SDK and target macOS 26.7 or newer; Metal shaders are embedded at build time.
- Standard native release targets use Metal on macOS and Vulkan on Linux and Windows. CUDA is an opt-in build, not a standard release target.

Build the single executable:

```sh
cargo build -p gpq
cargo build --release -p gpq
```

For the validated macOS target, set `MACOSX_DEPLOYMENT_TARGET=26.7` and select `--features metal`. Linux and Windows GPU builds select `--features vulkan`.

The release executable is `target/release/gpq`. Version-tag automation publishes native archives and checksums to the [GitHub Releases page](https://github.com/smartcrabai/gpq/releases/latest). After a native release has been published, its macOS Homebrew package can be installed with:

```sh
brew install smartcrabai/tap/gpq
```

The host needs its OS dynamic loader and a supported GPU driver; Vulkan Workers also need the OS Vulkan loader. Native inference, GGML, and media codecs are included in the executable. Workers do not need Python, an external inference server, ComfyUI, MLX, or an ffmpeg executable at runtime. Remote needs PostgreSQL but no GPU or model assets.

## PostgreSQL setup

Use separate PostgreSQL logins for migration, local administration, and serving:

1. The **migration login** owns the schema and can create the `gpq_admin` and `gpq_app` group roles. Native cutover requires effective inherited `gpq_admin` privileges or `BYPASSRLS`; `NOINHERIT` membership alone is insufficient.
2. The **administration login** is granted membership in `gpq_admin` and is used for local Tenant and Worker administration.
3. The **serving login** is a non-owner member of `gpq_app`, with `NOBYPASSRLS`; forced row-level security applies to tenant data.

Create the database and migration login through your normal PostgreSQL provisioning process. For a non-superuser migration login, provision `gpq_admin` first and grant it `CREATE` on the target schema: legacy migrations transfer authentication-function ownership to that role. Run migrations with the schema-owner connection:

```sh
GPQ_DATABASE_URL='postgres://migration-user:password@localhost/gpq' \
  target/release/gpq remote migrate
```

Migration `0008_native_cutover` is intentionally non-destructive: it refuses to run if legacy feature data exists and deletes no history. Back up and preserve the old database; migrate only a fresh or deliberately emptied feature database after archiving/exporting data you need. Do not expect this migration to convert old API/workflow records.

The migrations create any missing group roles. Create separate login roles and grant membership:

```sql
CREATE ROLE gpq_operator LOGIN PASSWORD 'replace-me';
GRANT gpq_admin TO gpq_operator;

CREATE ROLE gpq_server LOGIN PASSWORD 'replace-me' NOBYPASSRLS;
GRANT gpq_app TO gpq_server;
```

## Remote configuration

`gpq remote serve` and local administration require:

- `GPQ_DATABASE_URL`: connection string for the command; use the `gpq_app` serving login for `serve`, `gpq_admin` for local administration.
- `GPQ_CREDENTIAL_KEY`: 32 random bytes encoded as exactly 64 hexadecimal characters. Persist it securely; changing it invalidates stored Tenant and Worker credential hashes.
- `GPQ_PUBLIC_BASE_URL`: externally visible base URL used in Artifact download links; the shared configuration loader also requires it for local administration.

`GPQ_BIND_ADDR` is optional and defaults to `0.0.0.0:8080`. S3 settings (`GPQ_S3_BUCKET`, `GPQ_S3_REGION`, `GPQ_S3_ENDPOINT`, and `GPQ_S3_PRESIGN_TTL_SECS`) are optional and needed only for object-store placement. Use a secret manager or another secure mechanism for database passwords and credential keys.

Generate a credential hashing key once:

```sh
openssl rand -hex 32
```

Start Remote with the forced-RLS serving login:

```sh
export GPQ_DATABASE_URL='postgres://gpq_server:password@localhost/gpq'
export GPQ_CREDENTIAL_KEY='<64 hex characters>'
export GPQ_PUBLIC_BASE_URL='https://gpq.example.com'
export GPQ_BIND_ADDR='0.0.0.0:8080'

target/release/gpq remote serve
```

Readiness requires PostgreSQL; it does not require S3. The Tenant console at `/console` reads and updates queue age/capacity, Artifact limits, execution-timeout ceiling, and default priority through `TenantService`. Tenant and Worker lifecycle administration remains local.

## Tenant administration

Run local administration as a login granted `gpq_admin`, with the shared Remote configuration environment set:

```sh
export GPQ_DATABASE_URL='postgres://gpq_operator:password@localhost/gpq'
export GPQ_CREDENTIAL_KEY='<64 hex characters>'
export GPQ_PUBLIC_BASE_URL='https://gpq.example.com'

target/release/gpq remote tenant create --name example
target/release/gpq remote tenant key rotate \
  --tenant '<tenant-uuid>' \
  --label initial
```

The Master Key is printed once; store it securely. Other commands include:

```sh
target/release/gpq remote tenant list
target/release/gpq remote tenant delete --id '<tenant-uuid>'
target/release/gpq remote tenant key list --tenant '<tenant-uuid>'
target/release/gpq remote tenant key revoke --tenant '<tenant-uuid>' --key-id '<key-uuid>'
target/release/gpq remote worker list --tenant '<tenant-uuid>'
target/release/gpq remote worker revoke --tenant '<tenant-uuid>' --worker '<worker-uuid>'
```

## Worker configuration

Each Worker loads one TOML file at startup. Use absolute paths; Device Pools must not select overlapping devices. `backend` selects a native device supported by the compiled accelerator backend (for example, `metal`, `vulkan0`, `cuda0`, or `cpu`), not an external server; use the selector supported by your target host. A Pool's private `listen` address must be a loopback IP address. Set shape limits to match the GPU resources you intend to serve; the defaults shown below are limits, not a hardware guarantee.

This schematic config illustrates Qwen Image 2.1 with an optional compatible Turbo/style image LoRA and MiniMax H3 with the matched FlashGen LoRA. Qwen image editing is native and does not require a separate edit LoRA. Replace every example path with an existing absolute file path from the linked model sources. For production, pin each configured model and LoRA file under `[pools.expected_hashes]` using its exact 64-character SHA-256 digest:

```toml
name = "worker-1"
remote_url = "http://gpq.internal:8080"
state_dir = "/var/lib/gpq"
# On headless macOS/SSH hosts, optionally set `credential_storage = "file"` to use a private file instead of Keychain.

[[pools]]
key = "gpu0"
backend = "vulkan0"
listen = "127.0.0.1:39001"
state_dir = "/var/lib/gpq/gpu0"
startup_timeout_secs = 180
extra_image_loras = ["/models/qwen/qwen_image_2.1_turbo_lora_avg_rank_178_bf16.safetensors"]
# On Vulkan hosts that need explicit ICD discovery, set the per-Pool driver file path:
# [pools.env]
# VK_DRIVER_FILES = "/path/to/driver/icd.json"

[pools.shape]
max_image_pixels = 16777216
max_video_pixels = 64000000
max_video_frames = 97
max_steps = 100

[pools.expected_hashes]
"/models/qwen/diffusion-model.safetensors" = "<64-hex SHA-256>"
"/models/qwen/text-encoder.safetensors" = "<64-hex SHA-256>"
"/models/qwen/vision-encoder.safetensors" = "<64-hex SHA-256>"
"/models/qwen/vae.safetensors" = "<64-hex SHA-256>"
"/models/qwen/qwen_image_2.1_turbo_lora_avg_rank_178_bf16.safetensors" = "<64-hex SHA-256>"
"/models/h3/MiniMax-H3-FL2VA-Pruned-Q4_K_M.gguf" = "<64-hex SHA-256>"
"/models/h3/text-encoder.gguf" = "<64-hex SHA-256>"
"/models/h3/video-vae.gguf" = "<64-hex SHA-256>"
"/models/h3/audio-vae.gguf" = "<64-hex SHA-256>"
"/models/h3/flashgen-4step-lora.safetensors" = "<64-hex SHA-256>"

[[pools.models]]
family = "qwen_image_2_1"
[pools.models.assets]
diffusion = "/models/qwen/diffusion-model.safetensors"
text_encoder = "/models/qwen/text-encoder.safetensors"
vision_encoder = "/models/qwen/vision-encoder.safetensors"
vae = "/models/qwen/vae.safetensors"
[pools.models.sampling]
steps = 40
cfg_scale = 6.0
sigmas = []

[[pools.models]]
family = "minimax_h3"
[pools.models.assets]
diffusion = "/models/h3/MiniMax-H3-FL2VA-Pruned-Q4_K_M.gguf"
text_encoder = "/models/h3/text-encoder.gguf"
vae = "/models/h3/video-vae.gguf"
audio_vae = "/models/h3/audio-vae.gguf"
[[pools.models.default_loras]]
path = "/models/h3/flashgen-4step-lora.safetensors"
scale = 1.0
[pools.models.sampling]
steps = 4
cfg_scale = 1.0
sigmas = [1.0, 0.965517, 0.888889, 0.679245, 0.0]
```

The default pool shape limits are 16,777,216 image pixels, 64,000,000 video pixels, 97 video frames, and 100 image steps. A video request must satisfy both total video pixels (`width × height × frames`) and the frame limit. `runtime` options can configure threads, VAE tiling, model arguments, per-device parameter residency, VRAM budgets, and module-to-device assignments; use only values supported by the selected native backend. Worker hashes configured files into model manifests; aliases are assigned to a manifest hash advertised by an online Worker.
Hardware-tested profiles below report observed generation times, not service guarantees. Shape values are configured pool limits; the observed process-lifetime memory high-water mark is not a per-job peak or a memory guarantee.

| Host / backend | Completed generation samples | Pool limits / tested request timeouts | Runtime and memory observation |
| --- | --- | --- | --- |
| macOS 26.7, Apple M4 Max (48 GiB), `metal` / `MTL0` | Qwen Image: 512×512 base at 40 steps / CFG 6 — 276.88 s; 512×512 Turbo LoRA at 8 steps / CFG 1, scale 1 — 44.04 s (665/665 LoRA tensors applied); 512×512 reference edit — 306.52 s. H3 with matched FlashGen LoRA at 4 steps / CFG 1: first+last-conditioned 320×192, 22 frames (0.92 s output) — 92.05 s API elapsed (828/828 LoRA tensors applied); 320×192, 124 frames (5.17 s output) — 142.82 s. | `max_image_pixels=262144`; `max_steps=40`; `max_video_pixels=7618560`; `max_video_frames=124`; tested request execution timeout 1,800 s (not a pool field). | `params_backend="disk"`, `max_vram="metal0=24"` GiB, `threads=8`, `vae_tiling=true`; observed process-lifetime memory high-water mark 20 GB. |
| WSL2 Ubuntu, Radeon 780M, Mesa 26.0.8 Dozen (DZN; experimental, non-conformant Vulkan 1.2.335 via D3D12; not native Linux RADV) | On the original 12-GiB profile, Qwen Image base at 512×512 / 40 steps / CFG 6 — 3,075.86 s; Turbo LoRA scale 1 at 256×256 / 8 steps / CFG 1 — 317.11 s native (about 318 s API; 665/665 tensors applied); reference edit from a 256×256 source at 256×256 / 8 steps / CFG 1 — 537.4 s native (665/665 applied). On the shared 6-GiB profile, Qwen Turbo LoRA scale 1 at 256×256 / 8 steps / CFG 1 — 345.63 s wall (344.56 s native; 665/665 applied); reference edit with a 256×256 source at 256×256 / 8 steps / CFG 1 — 437.87 s wall (436.40 s native). The 6-GiB profile completed all three matched-LoRA H3 modes (text, first-frame, first+last-frame) at 256×160, 22 frames / 24 fps (0.92 s output), 4 steps / CFG 1; wall times were 2,364.61 s (about 39.41 minutes), 2,513.30 s (about 41.89 minutes), and 2,732.36 s (about 45.54 minutes), respectively. | 6-GiB profile: `max_image_pixels=262144`; `max_steps=40`; `max_video_pixels=901120`; `max_video_frames=22`. Request timeouts used: 3,600 s for Qwen image/edit and H3 text/first-frame; 7,200 s for H3 first+last-frame (request-level, not pool limits). | Diffusion on `vulkan0`; text encoder (`te`) and VAE (`vae`) on CPU; `params_backend="disk"`; original Qwen profile `max_vram="vulkan0=12"` GiB; shared profile `max_vram="vulkan0=6"` GiB; `threads=8`; `vae_tiling=true`; per-pool `VK_DRIVER_FILES`. |

## Worker enrollment and operation

Enrollment reads the Tenant Master Key from standard input only. The returned Worker Credential is stored using the configured platform secret store:

```sh
target/release/gpq worker enroll --config /etc/gpq/worker.toml
```

On macOS the default is Keychain; Windows uses Credential Manager. Linux uses systemd credentials for systemd services and otherwise an owner-only file. On Unix, `credential_storage = "file"` explicitly selects the owner-only `0600` credential file in the private mode-`0700` state directory, including for headless macOS hosts. There is no automatic fallback. Keep the Worker Credential separate from the Tenant Master Key.

Diagnose and run the Worker in the foreground:

```sh
target/release/gpq worker diagnose --config /etc/gpq/worker.toml
target/release/gpq worker run --config /etc/gpq/worker.toml
```

Diagnosis validates configured native devices in a short-lived isolated child with the pool's environment. It does not bind the pool listener or stop an active inference child, and unavailable devices make the command exit nonzero.

Install the same foreground command under the platform service manager:

```sh
target/release/gpq worker service install --config /etc/gpq/worker.toml
target/release/gpq worker service start
target/release/gpq worker service stop
target/release/gpq worker service uninstall --config /etc/gpq/worker.toml
```

Uninstalling also removes the stored Worker Credential. Service wrappers do not create a second Worker implementation.

## Native API example

The schemas are under [`proto/gpq/v1`](proto/gpq/v1). Native Connect RPCs use typed image/video request messages; `Submit` requires an idempotency key in request metadata. Use `CatalogService.ListModels` to find an advertised content hash and `SetModelAlias` to assign a Tenant-facing model alias.

[`examples/native-image-video`](examples/native-image-video/main.ts) is a Node.js 22.18+ client that generates a Qwen image, edits it through an uploaded input Artifact, then submits an H3 video generation using the image as first and last frames. It uses only the typed native API:

```sh
cd examples/native-image-video
GPQ_BASE_URL='https://gpq.example.com' \
GPQ_MASTER_KEY='<tenant-master-key>' \
GPQ_QWEN_IMAGE_ALIAS='qwen-image' \
GPQ_H3_VIDEO_ALIAS='h3-video' \
  node main.ts 'A red panda astronaut' --size 512x512 --frames 39 \
    --edit-prompt 'Add a glass helmet' --video-prompt 'The astronaut waves' \
    --seed 7 --out-dir ./out
```

The example uploads image bytes to Remote-local PostgreSQL storage when S3 is absent. Public generation does not invoke a Python, Node.js, or ffmpeg inference helper; Node.js is needed only to run this example client.

## Container image

The Linux OCI image contains the same `gpq` executable and runs the Remote role. Its default command is `gpq remote serve`; the image includes the Vulkan loader needed for binary startup. It is non-root and expects TLS termination outside the container.

```sh
docker build -t gpq .
docker run --rm --env-file remote-migrate.env gpq remote migrate
docker run --rm --env-file remote.env -p 8080:8080 gpq
```

The image does not run GPU inference; install `gpq` on each Worker host.

## Releases and development checks

Version tags publish one `gpq` executable in target-specific release archives with SHA-256 checksums and the Homebrew formula `gpq`. Current release targets are Apple Silicon macOS, x86_64/aarch64 Linux, and x86_64/aarch64 Windows. The same binary provides `remote`, `worker`, and private `infer` roles.

CI checks and PostgreSQL integration requirements are defined in [`.github/workflows/test.yml`](.github/workflows/test.yml). PostgreSQL integration tests require Docker.

Run checks with the platform's default GPU backend; `--all-features` selects backends that cannot coexist on every OS:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --bins --lib
cargo build --workspace --bins
cargo test -p gpq --test postgres --test e2e --test lifecycle --test objectstore
```

When the test process and Docker daemon run on different hosts, set `TESTCONTAINERS_HOST_OVERRIDE` to the reachable daemon-host address; guest loopback does not reach host-published ports. Native Unicode PNG/WebM output regression needs no model assets: configure the native CMake project with `GPQ_BUILD_MEDIA_TESTS=ON`, build `gpq_media_output_paths_test`, and run CTest in that build directory.

## Project layout

```text
crates/gpq          Single executable and role dispatch
crates/gpq-domain   Domain types, state transitions, and scheduling
crates/gpq-proto    Generated protocol bindings
crates/gpq-remote   Coordinator APIs, scheduler, PostgreSQL, and migrations
crates/gpq-worker   Worker session, config, process supervision, and native child
crates/gpq-inference Native inference bridge and media encoders
proto/gpq           Native public and Worker protocol schemas
docs/adr            Architecture decision records
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
