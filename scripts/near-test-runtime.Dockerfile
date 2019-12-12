FROM ubuntu:19.04

RUN apt-get update -qq && apt-get install -y \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*