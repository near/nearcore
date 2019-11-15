# syntax=docker/dockerfile-upstream:experimental

FROM phusion/baseimage:0.11

RUN apt-get update -qq && apt-get install -y \
    git \
    cmake \
    g++ \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    unzip \
    systemd-coredump \
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

ENV CARGO_TARGET_DIR=/tmp/target
ENV RUSTC_FLAGS='-C target-cpu=x86-64'
RUN --mount=type=cache,target=/tmp/target \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build -p near --release && \
    cargo build -p keypair-generator --release && \
    cargo build -p genesis-csv-to-json --release && \
    cp /tmp/target/release/near /usr/local/bin/ && \
    cp /tmp/target/release/keypair-generator /usr/local/bin && \
    cp /tmp/target/release/genesis-csv-to-json /usr/local/bin

EXPOSE 3030 24567

COPY scripts/run_docker.sh /usr/local/bin/run.sh

ENTRYPOINT ["/sbin/my_init", "--"]

CMD ["/usr/local/bin/run.sh"]
