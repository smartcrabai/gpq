# Remote primarily ships as a Linux OCI image; the Worker ships through
# cargo-dist instead (ADR 0020).

# Chef stage: Install cargo-chef
FROM lukemathwalker/cargo-chef:latest-rust-1.97@sha256:6dce65df3d7430c797e94348b4cf36d8d5876b63ca54f35dbfd37a97c42d0add AS chef

WORKDIR /app

# Protobuf Edition 2024 schemas require protoc 33 or newer (ADR 0004). Debian
# and Alpine both package older releases, so take the upstream binary.
ARG PROTOC_VERSION=35.1

RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
      amd64) protoc_arch=x86_64 ;; \
      arm64) protoc_arch=aarch_64 ;; \
      *) echo "unsupported architecture" >&2; exit 1 ;; \
    esac; \
    apt-get update; \
    apt-get install -y --no-install-recommends unzip; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL -o /tmp/protoc.zip \
      "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip"; \
    unzip -q /tmp/protoc.zip -d /usr/local; \
    rm /tmp/protoc.zip; \
    protoc --version

# Planner stage: Prepare recipe.json
FROM chef AS planner

COPY . .

RUN cargo chef prepare --recipe-path recipe.json

# Builder stage: Build dependencies and application
FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json

# Build dependencies (this layer will be cached)
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json

# Copy source code and build application
COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    cargo build --release --package gpq-remote && \
    cp /app/target/release/gpq-remote /tmp/gpq-remote

# Runtime stage: minimal glibc image, since the build links against the host C
# library rather than producing a fully static binary
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:d97bc0a941b8d4be647dc0ee75b264ddbb772f1ac5ba690a4309c00723b23775

WORKDIR /app

COPY --from=builder /tmp/gpq-remote .

# TLS terminates at the ingress; Remote speaks plaintext HTTP/1.1 and h2c
# (ADR 0019).
EXPOSE 8080

CMD ["./gpq-remote", "serve"]
