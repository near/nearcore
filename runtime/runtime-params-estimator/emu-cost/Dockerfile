# our local base image
FROM docker.io/rust:1.86.0

LABEL description="Container for builds"

RUN rustup target add wasm32-unknown-unknown x86_64-unknown-linux-gnu && rustup component add rustfmt clippy

# install build dependencies for QEMU
RUN apt-get update && apt-get install -y  g++ rsync zip openssh-server make apt-utils git sudo \
     pkg-config libglib2.0-dev curl clang gdb llvm-dev cmake python3-venv ninja-build

# cspell:ignore libglib
