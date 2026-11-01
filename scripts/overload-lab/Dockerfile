# The overload lab: rtcd and the load generator (lms-sim) in one image, to run in two containers
# with a real kernel send path between them (see README.md in this directory).
#
#   docker build -f scripts/overload-lab/Dockerfile -t rtcd-lab:NAME .
ARG RUST_VERSION=1.99.1
FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /src
COPY . .
# A lab build: thin LTO and one codegen unit are the release profile's; the lab trades a few
# percent of speed (the same for every run) for a build that takes minutes.
ENV CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
# The target directory is a cache shared between builds of different sources: make cargo see ours
# as new (COPY keeps the files' own times, which can be older than what the cache holds).
RUN find crates -name '*.rs' -exec touch {} +
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p rtcd -p lms-sim \
    && cp target/release/rtcd target/release/lms-sim /usr/local/bin/

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates iproute2 procps curl python3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/rtcd /usr/local/bin/lms-sim /usr/local/bin/
COPY deploy/config.template.toml /etc/rtcd/config.template.toml
COPY deploy/entrypoint.sh /usr/local/bin/rtcd-entrypoint.sh
COPY scripts/overload-lab/sample.py /usr/local/bin/sample.py
RUN chmod 0755 /usr/local/bin/rtcd-entrypoint.sh && mkdir -p /data /tmp/rtcd
WORKDIR /data
