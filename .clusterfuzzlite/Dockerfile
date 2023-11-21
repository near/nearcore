FROM gcr.io/oss-fuzz-base/base-builder-rust:v1
RUN apt-get update && apt-get install -y make autoconf automake libtool clang llvm
RUN rustup toolchain uninstall nightly && rustup toolchain install nightly
RUN rustup default nightly
RUN rustup target add wasm32-unknown-unknown --toolchain=nightly
RUN rustup component add rust-src
RUN cargo install cargo-fuzz
COPY . $SRC/nearcore
WORKDIR nearcore
COPY .clusterfuzzlite/build.sh $SRC/
