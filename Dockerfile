# Docker image
FROM ubuntu:22.04

EXPOSE 3030 24567

RUN apt-get update -qq && apt-get install -y \
    libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/run_docker.sh /usr/local/bin/run.sh
COPY neard /usr/local/bin/

CMD ["/usr/local/bin/run.sh"]
