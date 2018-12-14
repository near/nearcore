# syntax=docker/dockerfile-upstream:experimental

FROM alpine:edge AS builder

ARG PACKAGE="devnet"
ARG PROFILE="release"
ARG RELEASE_FLAG="--release"

RUN apk update && apk add --no-cache build-base \
    cargo \
    cmake \
    # needed for exonum_sodiumoxide build script
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
    cargo build -p $PACKAGE $RELEASE_FLAG

# Copy binaries into normal layers
RUN --mount=type=cache,target=/tmp/target \
    cp /tmp/target/$PROFILE/$PACKAGE /usr/local/bin/

# ===== SECOND STAGE ======

FROM alpine:edge

# Args must be defined for every stage
ARG PACKAGE="devnet"

RUN apk update && apk add --no-cache libstdc++ \
    openssl-dev \
    tini

RUN rm -rf /usr/lib/python* && \
	mkdir -p /root/.local/share/nearcore && \
	ln -s /root/.local/share/nearcore /data

COPY --from=builder /usr/local/bin/$PACKAGE /usr/local/bin

ENTRYPOINT ["/sbin/tini", "--"]

# This is required for supporting multiple packages
RUN ln -s /usr/local/bin/$PACKAGE /docker_command
CMD ["/docker_command"]
