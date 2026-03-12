ARG VERSION=2.10.7

FROM debian:bookworm-slim AS download-amd64
ARG VERSION
ADD https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/${VERSION}/near-sandbox.tar.gz /tmp/near-sandbox.tar.gz
RUN tar -xzf /tmp/near-sandbox.tar.gz --strip-components=1 -C /usr/local/bin && \
    chmod +x /usr/local/bin/near-sandbox

FROM debian:bookworm-slim AS download-arm64
ARG VERSION
ADD https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-aarch64/${VERSION}/near-sandbox.tar.gz /tmp/near-sandbox.tar.gz
RUN tar -xzf /tmp/near-sandbox.tar.gz --strip-components=1 -C /usr/local/bin && \
    chmod +x /usr/local/bin/near-sandbox

ARG TARGETARCH
# hadolint ignore=DL3006
FROM download-${TARGETARCH} AS final

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3030

ENTRYPOINT ["entrypoint.sh"]
