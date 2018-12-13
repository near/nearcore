# syntax=docker/dockerfile-upstream:experimental

FROM alpine:edge AS builder

RUN apk update && apk add build-base \
    cargo \
    # needed for exonum_sodiumoxide build script
    cmake \
    curl \
    linux-headers \
    openssl-dev

WORKDIR /nearcore
COPY . /nearcore

# Build with mounted cache
ENV CARGO_HOME=/usr/local/cargo
ENV CARGO_TARGET_DIR=/tmp/target
RUN --mount=type=cache,target=/tmp/target \
    --mount=type=cache,target=/usr/local/cargo \
    ["cargo", "build", "-p", "devnet", "--release"]

# Copy binaries into normal layers
RUN --mount=type=cache,target=/tmp/target \
    ["cp", "/tmp/target/release/devnet", "/usr/local/bin/devnet"]

# ===== SECOND STAGE ======

FROM alpine:edge

RUN apk add --no-cache ca-certificates \
    libstdc++ \
    openssl-dev \
    tini

RUN rm -rf /usr/lib/python* && \
	mkdir -p /root/.local/share/nearcore && \
	ln -s /root/.local/share/nearcore /data

COPY --from=builder /usr/local/bin/devnet /usr/local/bin

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/usr/local/bin/devnet"]
