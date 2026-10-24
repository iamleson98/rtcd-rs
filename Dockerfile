# The Rust rtcd as a drop-in for the LMS's Go image: same entrypoint, same config template, same
# RTCD_* environment, ports (8045 API, 8443 media), user (2001) and data volume (/data).
#
#   docker build -t lms-rtcd-rust compare/rtcd-rust      # needs the vendor/rtc submodule checked out
#
# Build on the machine's own architecture; nothing here is architecture specific.
# The latest stable Rust (2026-10-13); raise it with --build-arg RUST_VERSION=...
ARG RUST_VERSION=1.99.1
FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p rtcd \
    && cp target/release/rtcd /rtcd

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 2001 rtcd \
    && useradd --system --no-create-home --uid 2001 --gid rtcd rtcd \
    && mkdir -p /data /tmp/rtcd \
    && chown -R rtcd:rtcd /data /tmp/rtcd

COPY --from=builder /rtcd /usr/local/bin/rtcd
COPY deploy/config.template.toml /etc/rtcd/config.template.toml
COPY deploy/entrypoint.sh /usr/local/bin/rtcd-entrypoint.sh
RUN chmod 0755 /usr/local/bin/rtcd /usr/local/bin/rtcd-entrypoint.sh

USER rtcd
WORKDIR /data

# 8045 = control API (HTTP + WebSocket, internal networks only)
# 8443 = ICE media (UDP + TCP, must be published in host mode)
EXPOSE 8045 8443/tcp 8443/udp
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD wget -q -O /dev/null http://localhost:8045/version || exit 1

ENTRYPOINT ["/usr/local/bin/rtcd-entrypoint.sh"]
