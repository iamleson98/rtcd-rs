# Production image of the Rust rtcd (the WebRTC SFU). Published by .github/workflows/image.yml as
# ghcr.io/<owner>/rtcd-rust on a version tag.
#
#   git submodule update --init --recursive      # vendor/rtc, the WebRTC stack it builds on
#   docker build -t rtcd-rust .
#   docker buildx build --platform linux/amd64,linux/arm64 -t rtcd-rust .
#
# The image carries no configuration file: mount one at /etc/rtcd/rtcd.toml (deploy/rtcd.toml is the
# reference; a Kubernetes ConfigMap in production). The RTCD_* environment variables override it, so
# changing the advertised ICE IP is a manifest change, not an image rebuild. rtcd reads both itself:
# there is no entrypoint script.

# ────────────────────────────── Builder ──────────────────────────────
# The latest stable Rust when this was written; raise it with --build-arg RUST_VERSION=...
ARG RUST_VERSION=1.99.1
# The builder runs on the machine's own platform and cross-compiles for the one the image is for
# (BuildKit sets TARGETARCH from --platform): an arm64 laptop building the amd64 image needs no
# emulation, and on an amd64 runner nothing is cross-compiled.
FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-bookworm AS builder
ARG TARGETARCH
ARG RUST_VERSION
# What GET /version reports: image.yml passes the tag, the date and the commit.
ARG BUILD_NUMBER=dev
ARG BUILD_DATE=unknown
ARG BUILD_HASH=none

WORKDIR /src

# Layer-cache the registry downloads between builds; the build cache outlives the layer, so a change
# to one crate recompiles that crate, not the whole tree.
COPY . ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -eux; \
    case "${TARGETARCH}" in \
      amd64) triple=x86_64-unknown-linux-gnu;  gcc_pkg=gcc-x86-64-linux-gnu;  libc_pkg=libc6-dev-amd64-cross; gcc_bin=x86_64-linux-gnu-gcc ;; \
      arm64) triple=aarch64-unknown-linux-gnu; gcc_pkg=gcc-aarch64-linux-gnu; libc_pkg=libc6-dev-arm64-cross; gcc_bin=aarch64-linux-gnu-gcc ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}"; exit 1 ;; \
    esac; \
    rustup target add "${triple}"; \
    if [ "$(uname -m)" != "${triple%%-*}" ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends "${gcc_pkg}" "${libc_pkg}"; \
      upper="$(echo "${triple}" | tr 'a-z-' 'A-Z_')"; \
      export "CARGO_TARGET_${upper}_LINKER=${gcc_bin}" "CC_$(echo "${triple}" | tr '-' '_')=${gcc_bin}"; \
    fi; \
    RTCD_BUILD_VERSION="${BUILD_NUMBER}" RTCD_BUILD_DATE="${BUILD_DATE}" RTCD_BUILD_HASH="${BUILD_HASH}" \
    RTCD_RUSTC_VERSION="${RUST_VERSION}" \
    cargo build --release --locked -p rtcd --target "${triple}"; \
    cp "target/${triple}/release/rtcd" /rtcd

# ────────────────────────────── Runtime ──────────────────────────────
# A dated tag and its digest: the base never changes under a rebuild of the same commit.
FROM debian:bookworm-20261104-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

# ca-certificates: TLS for STUN/TURN over TLS and outbound HTTPS
# tzdata: correct log timestamps
# wget: the health check
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 2001 rtcd \
    && useradd --system --no-create-home --uid 2001 --gid rtcd rtcd \
    && mkdir -p /data \
    && chown rtcd:rtcd /data

COPY --from=builder /rtcd /usr/local/bin/rtcd

RUN chmod 0755 /usr/local/bin/rtcd

USER rtcd
WORKDIR /data

# 8045 = control API (HTTP + WebSocket, internal networks only)
# 8443 = ICE media (UDP + TCP, must be published in host mode)
EXPOSE 8045 8443/tcp 8443/udp

VOLUME ["/data"]

# rtcd's /version endpoint is an unauthenticated GET.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8045/version || exit 1

ENTRYPOINT ["/usr/local/bin/rtcd"]
CMD ["--config", "/etc/rtcd/rtcd.toml"]
