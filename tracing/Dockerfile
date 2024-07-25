FROM rust:1.80-buster as builder

ADD Cargo.toml /build/Cargo.toml
ADD Cargo.lock /build/Cargo.lock
ADD src /build/src
WORKDIR /build
RUN cargo build --release

FROM debian:buster-slim
COPY --from=builder /build/target/release/near-tracing /app/near-tracing
CMD ["/app/near-tracing"]
