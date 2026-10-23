# The single gpq executable ships as this Linux OCI image (`gpq remote serve`)
# and through cargo-dist (ADR 0020).

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
    apt-get install -y --no-install-recommends unzip cmake g++ git libvulkan-dev glslc; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL -o /tmp/protoc.zip \
      "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${protoc_arch}.zip"; \
    unzip -q /tmp/protoc.zip -d /usr/local; \
    rm /tmp/protoc.zip; \
    protoc --version

# SPIRV-Headers is built from the pinned upstream source so its CMake config
# (share/cmake/SPIRV-Headers) is present for gpq-inference.
COPY scripts/install-spirv-headers.sh /tmp/install-spirv-headers.sh
RUN bash /tmp/install-spirv-headers.sh /opt/spirv-headers && rm /tmp/install-spirv-headers.sh
ENV CMAKE_PREFIX_PATH=/opt/spirv-headers

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
    cargo build --release --package gpq && \
    cp /app/target/release/gpq /tmp/gpq && \
    cp -L /usr/lib/*-linux-gnu/libvulkan.so.1 /tmp/libvulkan.so.1

# Runtime stage: minimal glibc image, since the build links against the host C
# library rather than producing a fully static binary
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:54df941ed0d06a1bd95ef5e0ce391fd8d9f94b64782dc9a60062727849ee3f97

WORKDIR /app

COPY --from=builder /tmp/gpq .
# The gpq binary links the Vulkan loader (standard Linux backend). Remote never
# dispatches GPU work, but the loader must resolve at startup, and distroless
# ships none.
COPY --from=builder /tmp/libvulkan.so.1 /usr/lib/libvulkan.so.1

# TLS terminates at the ingress; Remote speaks plaintext HTTP/1.1 and h2c
# (ADR 0019).
EXPOSE 8080

CMD ["./gpq", "remote", "serve"]
