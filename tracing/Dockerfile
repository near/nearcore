# Please update when bumping the rust version in Cargo.toml. Both stages must
# use the same Debian release. The rust image drops a Debian variant once that
# release is no longer supported, so check that the tag exists.
FROM rust:1.100.0-trixie as builder

ADD Cargo.toml /build/Cargo.toml
ADD Cargo.lock /build/Cargo.lock
ADD src /build/src
WORKDIR /build
RUN cargo build --release

FROM debian:trixie-slim
COPY --from=builder /build/target/release/near-tracing /app/near-tracing
CMD ["/app/near-tracing"]
