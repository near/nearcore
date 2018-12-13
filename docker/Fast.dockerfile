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
    ["cargo", "build"]

# Copy binaries into normal layers
RUN --mount=type=cache,target=/tmp/target \
    ["cp", "/tmp/target/debug/nearcore", "/usr/local/bin/nearcore"]

# ===== SECOND STAGE ======

FROM alpine:edge

COPY --from=builder /usr/local/bin/nearcore /usr/local/bin

RUN apk add --no-cache ca-certificates \
    libstdc++ \
    openssl-dev

RUN rm -rf /usr/lib/python* && \
	mkdir -p /root/.local/share/nearcore && \
	ln -s /root/.local/share/nearcore /data

EXPOSE 30333 9933 9944
VOLUME ["/data"]

CMD ["/usr/local/bin/nearcore"]
