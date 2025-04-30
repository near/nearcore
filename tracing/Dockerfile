# Please update when bumping rust version
FROM rust:1.86.0-bullseye as builder

ADD Cargo.toml /build/Cargo.toml
ADD Cargo.lock /build/Cargo.lock
ADD src /build/src
WORKDIR /build
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder /build/target/release/near-tracing /app/near-tracing
CMD ["/app/near-tracing"]
