FROM gcr.io/oss-fuzz-base/base-builder-rust:v1
RUN apt-get update && apt-get install -y make autoconf automake libtool clang llvm
ARG CACHEBUST=2
RUN rustup default nightly
RUN rustup uninstall nightly && rustup install nightly
RUN rustup target add wasm32-unknown-unknown --toolchain=nightly
RUN rustup component add rust-src
RUN cargo install cargo-fuzz
RUN rustup default nightly
COPY . $SRC/nearcore
WORKDIR nearcore
COPY .clusterfuzzlite/build.sh $SRC/
