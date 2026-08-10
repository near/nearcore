# syntax=docker/dockerfile-upstream:experimental

FROM ubuntu:22.04 as build

RUN apt-get update -qq && apt-get install -y \
    git \
    cmake \
    g++ \
    pkg-config \
    libssl-dev \
    curl \
    llvm \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

VOLUME [ /near ]
WORKDIR /near
COPY . .

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl https://sh.rustup.rs -sSf | \
    sh -s -- -y --no-modify-path --default-toolchain none

RUN rustup toolchain install

ENV PORTABLE=ON
ARG make_target=
# The compiler daemon is experimental, pass build_compiler_daemon=1 to include it.
ARG build_compiler_daemon=0
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/tmp/target \
    make CARGO_TARGET_DIR=/tmp/target BUILD_COMPILER_DAEMON="${build_compiler_daemon}" "${make_target:?make_target not set}" && \
    mkdir /near/bin && \
    cp /tmp/target/release/neard /near/bin/ && \
    if [ "${build_compiler_daemon}" = "1" ]; then cp /tmp/target/release/near-vm-compiler-daemon /near/bin/; fi

# Docker image
FROM ubuntu:22.04

EXPOSE 3030 24567

RUN apt-get update -qq && apt-get install -y \
    libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/run_docker.sh /usr/local/bin/run.sh
COPY --from=build /near/bin/ /usr/local/bin/

CMD ["/usr/local/bin/run.sh"]
