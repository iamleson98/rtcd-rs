# The lesson soak's client side: `lms-sim` with its media made at build time, the process sampler and
# the tools the network shaping (tc) and the waits (curl) need. Build from the repository root:
#
#   docker build -f scripts/lesson-soak/Dockerfile -t rtcd-lesson:soak .
#
# Builds natively on linux/amd64 and linux/arm64.
ARG RUST_VERSION=1.99.1
FROM rust:${RUST_VERSION}-bookworm AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# A test build: thin LTO and one codegen unit are the release profile's; the clients' speed is the
# same for every run, and the build takes minutes instead of tens of them.
ENV CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p lms-sim \
    && cp target/release/lms-sim /usr/local/bin/ \
    && lms-sim assets /assets

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates iproute2 kmod procps curl python3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/lms-sim /usr/local/bin/
COPY --from=builder /assets /assets
COPY scripts/lesson-soak/sample.py /usr/local/bin/sample.py
ENV LMS_SIM_ASSETS=/assets
