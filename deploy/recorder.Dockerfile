# The LMS session recorder (one container per recording, launched by the server's recorderjob): a
# static binary on `scratch`. It decodes and encodes nothing, so the image needs no browser, no ffmpeg
# and no shell; it carries the TLS roots (the server may be reached over https) and the binary.
#
#   docker build -f deploy/recorder.Dockerfile -t lms-recorder .      # needs vendor/rtc checked out
#
# The builder runs on the machine's own platform and cross-compiles for the one the image is for
# (BuildKit sets TARGETARCH from --platform): an arm64 laptop building the amd64 image needs no
# emulation. zig is the C cross-compiler for `ring`'s few C and assembly files.

# The latest stable Rust when this was written; raise it with --build-arg RUST_VERSION=...
ARG RUST_VERSION=1.99.1
FROM --platform=$BUILDPLATFORM rust:${RUST_VERSION}-bookworm AS builder
ARG TARGETARCH
ARG RUST_VERSION
# What `recorder version` reports.
ARG BUILD_HASH=none

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && pip3 install --break-system-packages ziglang \
    && cargo install --locked cargo-zigbuild \
    && python3 -m ziglang version && cargo-zigbuild --version \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -eux; \
    case "${TARGETARCH}" in \
      amd64) triple=x86_64-unknown-linux-musl ;; \
      arm64) triple=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}"; exit 1 ;; \
    esac; \
    rustup target add "${triple}"; \
    RECORDER_REVISION="${BUILD_HASH}" \
    CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_STRIP=true \
    CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_PANIC=abort \
    cargo zigbuild --release --locked -p recorder --target "${triple}"; \
    cp "target/${triple}/release/recorder" /recorder; \
    install -d -o 10001 -g 10001 /out/data

# The TLS roots come from the builder's Debian.
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /recorder /recorder
COPY --from=builder --chown=10001:10001 /out/data /data
USER 10001:10001
ENTRYPOINT ["/recorder"]
