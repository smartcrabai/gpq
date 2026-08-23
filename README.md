# gpq

GPU Generation Queue (`gpq`) coordinates tenant-owned GPU workers and AI generation requests while prioritizing GPU utilization. It provides OpenAI-compatible HTTP APIs, a durable native Connect API, and workers that supervise local llama.cpp and ComfyUI processes.

## Architecture

- **`gpq-remote`** is the coordinator. PostgreSQL is its durable queue and metadata store.
- **`gpq-worker`** runs on each GPU host, opens an outbound control session to Remote, supervises backend processes, and executes leased attempts.
- **Tenants** own their Generations and Workers. A Worker never executes another Tenant's work.
- **Device Pools** are non-overlapping GPU sets. Each Pool runs one managed backend process and advertises its available execution slots.
- **Generations** use leased, at-least-once execution. Each retry creates a distinct Attempt; the first committed success becomes the accepted result.
- **Artifacts** are transient. Inputs expire after the Generation terminates; outputs are consumed once or expire independently.

The initial deployment model uses one Remote instance. PostgreSQL enables restart recovery; Worker reconnection and lease expiry recover interrupted work.

## API surfaces

All surfaces share one listener:

- OpenAI-compatible HTTP:
  - `GET /v1/models`
  - `POST /v1/chat/completions`
  - `POST /v1/responses`
- Native Connect services:
  - `gpq.v1.GenerationService`
  - `gpq.v1.CatalogService`
  - `gpq.v1.TenantService`
- Artifact download: `GET /v1/artifacts/{artifact_id}`
- Worker enrollment, control, and transfer services over gRPC
- Health checks: `GET /healthz` and `GET /readyz`

Public and native APIs use `Authorization: Bearer <Tenant Master Key>`. Worker protocol calls use a Worker Credential issued during enrollment.

TLS terminates at the ingress. `gpq-remote` itself serves plaintext HTTP/1.1 and h2c.

## Requirements

- Rust 1.97 or newer
- `protoc` 33 or newer; CI uses 35.1
- PostgreSQL; integration tests use PostgreSQL 18
- Docker for database and object-store integration tests
- llama.cpp and/or ComfyUI installed on Worker hosts
- Optional S3-compatible object storage for Native input Artifacts and object-store output placement

## Build

```sh
cargo build --workspace --bins
```

The binaries are written to:

```text
target/debug/gpq-remote
target/debug/gpq-worker
```

For optimized binaries:

```sh
cargo build --release --workspace --bins
```

## PostgreSQL setup

GPQ separates three database credentials:

1. **Migration credential**: owns the schema and can create the `gpq_admin` and `gpq_app` roles.
2. **Administration credential**: a login role granted membership in `gpq_admin`; used by local Tenant, key, and Worker administration commands.
3. **Serving credential**: a non-owner login role granted membership in `gpq_app`, without `BYPASSRLS`; used by `gpq-remote serve`.

Create the database and migration login using your normal PostgreSQL provisioning process. Run migrations with the schema-owner connection:

```sh
GPQ_DATABASE_URL='postgres://migration-user:password@localhost/gpq' \
  target/release/gpq-remote migrate
```

The migration creates the `gpq_admin` and `gpq_app` group roles. Create separate login roles and grant them membership:

```sql
CREATE ROLE gpq_operator LOGIN PASSWORD 'replace-me';
GRANT gpq_admin TO gpq_operator;

CREATE ROLE gpq_server LOGIN PASSWORD 'replace-me' NOBYPASSRLS;
GRANT gpq_app TO gpq_server;
```

Use a secret manager or your platform's credential mechanism instead of keeping database passwords in shell history or committed files.

## Remote configuration

Required environment variables:

- `GPQ_DATABASE_URL`: PostgreSQL connection string for the command being run.
- `GPQ_CREDENTIAL_KEY`: exactly 32 random bytes encoded as 64 hexadecimal characters. Persist this key; changing it invalidates stored Tenant and Worker credential hashes.
- `GPQ_PUBLIC_BASE_URL`: externally visible base URL used in Artifact download links.

Optional environment variables:

- `GPQ_BIND_ADDR`: listener address; default `0.0.0.0:8080`.
- `GPQ_S3_BUCKET`: S3-compatible Artifact bucket.
- `GPQ_S3_REGION`: required when `GPQ_S3_BUCKET` is set.
- `GPQ_S3_ENDPOINT`: endpoint override for non-AWS S3 implementations.
- `GPQ_S3_PRESIGN_TTL_SECS`: presigned URL lifetime; default `900`.
- Standard AWS SDK credential variables or workload identity settings when S3 is enabled.
- `RUST_LOG`: tracing filter; default `info`.
- Standard OpenTelemetry OTLP variables, such as `OTEL_EXPORTER_OTLP_ENDPOINT`.

Generate the credential hashing key once and store it securely:

```sh
openssl rand -hex 32
```

Start Remote using the serving database credential:

```sh
export GPQ_DATABASE_URL='postgres://gpq_server:password@localhost/gpq'
export GPQ_CREDENTIAL_KEY='<64 hex characters>'
export GPQ_PUBLIC_BASE_URL='https://gpq.example.com'

target/release/gpq-remote serve
```

Readiness requires PostgreSQL. Object storage is optional and does not affect `/readyz`.

## Tenant administration

Administration commands require the `gpq_admin` database credential plus the same `GPQ_CREDENTIAL_KEY` used by Remote.

Create a Tenant:

```sh
GPQ_DATABASE_URL='postgres://gpq_operator:password@localhost/gpq' \
  target/release/gpq-remote tenant create --name example
```

The command prints the Tenant UUID. Issue a Tenant Master Key:

```sh
GPQ_DATABASE_URL='postgres://gpq_operator:password@localhost/gpq' \
  target/release/gpq-remote tenant key rotate \
  --tenant '<tenant-uuid>' \
  --label initial
```

The secret is printed exactly once. Store it immediately.

Other administration commands:

```sh
gpq-remote tenant list
gpq-remote tenant delete --id '<tenant-uuid>'
gpq-remote tenant key list --tenant '<tenant-uuid>'
gpq-remote tenant key revoke --tenant '<tenant-uuid>' --key-id '<key-uuid>'
gpq-remote worker list --tenant '<tenant-uuid>'
gpq-remote worker revoke --tenant '<tenant-uuid>' --worker '<worker-uuid>'
```

Set `GPQ_DATABASE_URL`, `GPQ_CREDENTIAL_KEY`, and `GPQ_PUBLIC_BASE_URL` for each command. `GPQ_PUBLIC_BASE_URL` is required by the shared configuration loader even for local administration.

## Worker configuration

Workers use one TOML file loaded at startup. Paths must be absolute. Device selectors may not overlap between Pools.

Example llama.cpp Worker:

```toml
name = "worker-1"
remote_url = "http://gpq-remote.internal:8080"
state_dir = "/var/lib/gpq-worker"

[[pools]]
key = "gpu0"
backend = "llama_cpp"
executable = "/opt/llama.cpp/llama-server"
args = [
  "--host", "127.0.0.1",
  "--port", "8081",
  "--models-dir", "/models"
]
state_dir = "/var/lib/gpq-worker/gpu0"
startup_timeout_secs = 60
base_url = "http://127.0.0.1:8081"
slots = 4
model_paths = ["/models/example.gguf"]

[pools.env]
CUDA_VISIBLE_DEVICES = "0"

# Optional: reject startup when a configured model file does not match.
[pools.expected_hashes]
"/models/example.gguf" = "<64-character SHA-256 digest>"
```

Example additional ComfyUI Pool:

```toml
[[pools]]
key = "gpu1"
backend = "comfyui"
executable = "/opt/ComfyUI/.venv/bin/python"
args = [
  "/opt/ComfyUI/main.py",
  "--listen", "127.0.0.1",
  "--port", "8188"
]
state_dir = "/var/lib/gpq-worker/gpu1"
startup_timeout_secs = 120
base_url = "http://127.0.0.1:8188"
model_paths = []

[pools.env]
CUDA_VISIBLE_DEVICES = "1"
```

Backend arguments are passed directly to the executable; no shell interprets them. The configured `base_url` must match the backend's listen address.

## Worker enrollment and operation

Enrollment reads the Tenant Master Key from standard input only and stores the returned Worker Credential in the platform secret store:

```sh
target/release/gpq-worker enroll --config /etc/gpq-worker.toml
```

Run diagnostics before starting work:

```sh
target/release/gpq-worker diagnose --config /etc/gpq-worker.toml
```

Run in the foreground:

```sh
target/release/gpq-worker run --config /etc/gpq-worker.toml
```

Install the same foreground command under the platform service manager:

```sh
target/release/gpq-worker service install --config /etc/gpq-worker.toml
target/release/gpq-worker service start
```

Stop or remove it:

```sh
target/release/gpq-worker service stop
target/release/gpq-worker service uninstall --config /etc/gpq-worker.toml
```

Uninstalling also removes the stored Worker Credential.

## OpenAI-compatible usage

Create model aliases through `gpq.v1.CatalogService` after a Worker has advertised the matching model version. Then use the Tenant Master Key as the OpenAI API key.

List available model aliases:

```sh
curl 'https://gpq.example.com/v1/models' \
  -H 'Authorization: Bearer <tenant-master-key>'
```

Create a Chat Completion:

```sh
curl 'https://gpq.example.com/v1/chat/completions' \
  -H 'Authorization: Bearer <tenant-master-key>' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "example-model",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

Streaming uses the standard `"stream": true` request field. `POST /v1/responses` accepts the corresponding OpenAI Responses API shape. OpenAI image content parts may use `http:`, `https:`, or `data:` URLs; Remote rejects private and otherwise unsafe network targets.

## Native API

The Native Connect schemas live under [`proto/gpq/v1`](proto/gpq/v1):

- `GenerationService`: submit, inspect, list, cancel, watch, and create input Artifacts.
- `CatalogService`: register immutable Workflow Versions, manage Model and Workflow aliases, and list Workers.
- `TenantService`: read and update queue, capacity, Artifact, timeout, and priority settings.

Native Generation submission requires an idempotency key in request metadata. Model and Workflow aliases resolve to immutable content hashes at admission, so later alias changes do not mutate queued or completed Generations.

Native input Artifacts and object-store output placement require S3 configuration. Worker-local output Artifacts are downloaded once through `/v1/artifacts/{artifact_id}`; concurrent delivery conflicts, and consumed or expired Artifacts return `410 Gone`.

## Container image

The Docker image contains `gpq-remote` only:

```sh
docker build -t gpq-remote .
```

Run migrations by overriding the image command:

```sh
docker run --rm --env-file remote-migrate.env gpq-remote migrate
```

Run the server:

```sh
docker run --rm \
  --env-file remote.env \
  -p 8080:8080 \
  gpq-remote
```

The runtime image is non-root and expects TLS termination outside the container.

## Testing

Fast checks:

```sh
cargo fmt -- --check
cargo check --all-features
cargo test --all-features --bins --lib
cargo clippy --all-targets --all-features -- -D warnings
buf lint
```

Integration suites require Docker and must run serially within each test binary because each suite owns shared fixtures:

```sh
for suite in postgres e2e comfy lifecycle objectstore; do
  cargo test -p gpq-remote --test "$suite" -- --test-threads=1
done
```

CI also checks Cargo manifest formatting and Protobuf breaking changes against `main`.

## Project layout

```text
crates/gpq-domain   Domain types, state transitions, scheduling rules
crates/gpq-proto    Generated protocol bindings
crates/gpq-remote   Coordinator, APIs, scheduler, persistence, migrations
crates/gpq-worker   Worker session, process supervision, backend adapters
proto/gpq           Protobuf Edition 2024 schemas
docs/adr            Architecture decision records
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
