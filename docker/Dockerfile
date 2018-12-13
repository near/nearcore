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
RUN cargo build

# ===== SECOND STAGE ======

FROM alpine:3.8

COPY --from=builder /usr/local/bin/substrate /usr/local/bin

RUN apk add --no-cache ca-certificates \
    libstdc++ \
    openssl

RUN rm -rf /usr/lib/python* && \
	mkdir -p /root/.local/share/nearcore && \
	ln -s /root/.local/share/nearcore /data

EXPOSE 30333 3030
VOLUME ["/data"]

CMD ["/usr/local/bin/nearcore"]
