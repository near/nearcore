# syntax=docker/dockerfile-upstream:experimental

FROM phusion/baseimage:0.11

RUN apt-get update -qq && apt-get install -y \
    cmake \
    g++ \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    unzip \
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
RUN cargo build -p nearmint --release && \
    cp /tmp/target/release/nearmint /usr/local/bin/ && \
    cargo build -p keystore --release && \
    cp /tmp/target/release/keystore /usr/local/bin/ && \
    cargo build -p testlib --bin generate-test-spec --release && \
    cp /tmp/target/release/generate-test-spec /usr/local/bin/

RUN curl -L https://github.com/tendermint/tendermint/releases/download/v0.31.5/tendermint_v0.31.5_linux_amd64.zip > tendermint_v0.31.5_linux_amd64.zip
RUN unzip -o tendermint_v0.31.5_linux_amd64.zip -d /usr/local/bin/

EXPOSE 3030 26656 26657

COPY ops/run.sh /usr/local/bin/run.sh
COPY ops/tendermint-config.toml ./config.toml

ENTRYPOINT ["/sbin/my_init", "--"]

CMD ["/usr/local/bin/run.sh"]
