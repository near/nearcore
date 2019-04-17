# syntax=docker/dockerfile-upstream:experimental

FROM phusion/baseimage:0.11 AS builder

RUN apt-get update -qq && apt-get install -y \
    cmake \
    g++ \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=nightly

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- -y --no-modify-path --default-toolchain $RUST_VERSION

VOLUME [ /near ]
WORKDIR /near
COPY . .

VOLUME [ /tmp/target ]
ENV CARGO_TARGET_DIR=/tmp/target

RUN cargo build --release

EXPOSE 3030 26656 26657
