# syntax=docker/dockerfile-upstream:experimental

FROM phusion/baseimage:0.11 AS builder

ARG PACKAGE="devnet"
ARG PROFILE="release"
ARG RELEASE_FLAG="--release"

RUN apt-get update -qq && apt-get install -y \
    cmake \
    g++ \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Build with mounted cache
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.32.0

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- -y --no-modify-path --default-toolchain $RUST_VERSION

WORKDIR /nearcore
COPY . /nearcore

ENV CARGO_TARGET_DIR=/tmp/target
RUN --mount=type=cache,target=/tmp/target \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build -p $PACKAGE $RELEASE_FLAG

# Copy binaries into normal layers
RUN --mount=type=cache,target=/tmp/target \
    cp /tmp/target/$PROFILE/$PACKAGE /usr/local/bin/

# ===== SECOND STAGE ======

FROM phusion/baseimage:0.11

# Args must be defined for every stage
ARG PACKAGE="devnet"

RUN mkdir -p /root/.local/share/nearcore && \
	ln -s /root/.local/share/nearcore /data

COPY --from=builder /usr/local/bin/$PACKAGE /usr/local/bin

ENTRYPOINT ["/sbin/my_init", "--"]

# This is required for supporting multiple packages
RUN ln -s /usr/local/bin/$PACKAGE /docker_command
CMD ["/docker_command"]
