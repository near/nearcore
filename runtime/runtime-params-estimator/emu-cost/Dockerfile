# our local base image
FROM rust:1.71.0

LABEL description="Container for builds"

RUN rustup target add wasm32-unknown-unknown

# install build dependencies for QEMU
RUN apt-get update && apt-get install -y  g++ rsync zip openssh-server \
     make apt-utils git sudo pkg-config libglib2.0-dev curl clang gdb llvm-dev cmake
