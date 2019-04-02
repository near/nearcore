# syntax=docker/dockerfile-upstream:experimental

FROM phusion/baseimage:0.11 AS builder

ARG PACKAGE="devnet"
ARG PROFILE="release"
ARG RELEASE_FLAG="--release"

RUN apt-get update -qq && apt-get install -y \
    cmake \
    g++ \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Build with mounted cache
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=nightly

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- -y --no-modify-path --default-toolchain $RUST_VERSION

WORKDIR /nearcore
COPY . /nearcore

ENV CARGO_TARGET_DIR=/tmp/target
RUN --mount=type=cache,target=/tmp/target \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build -p $PACKAGE $RELEASE_FLAG && \
    cp /tmp/target/$PROFILE/$PACKAGE /usr/local/bin/ && \
    cargo build -p testlib --bin generate-test-spec $RELEASE_FLAG && \
    cp /tmp/target/$PROFILE/generate-test-spec /usr/local/bin/ && \
    cargo build -p keystore $RELEASE_FLAG && \
    cp /tmp/target/$PROFILE/keystore /usr/local/bin/

# ===== SECOND STAGE ======

FROM phusion/baseimage:0.11

# Args must be defined for every stage
ARG PACKAGE="devnet"

RUN apt-get update -qq && apt-get install -y \
    dnsutils \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/.local/share/nearcore && \
	ln -s /root/.local/share/nearcore /data

COPY --from=builder /usr/local/bin/$PACKAGE /usr/local/bin
COPY --from=builder /usr/local/bin/generate-test-spec /usr/local/bin
COPY --from=builder /usr/local/bin/keystore /usr/local/bin

COPY docker/scripts/hosted_alphanet_chain_spec.json /tmp/hosted_alphanet_chain_spec.json
COPY docker/scripts/run_alphanet /usr/local/bin/run_alphanet

ENTRYPOINT ["/sbin/my_init", "--"]

CMD ["/usr/local/bin/run_alphanet"]
