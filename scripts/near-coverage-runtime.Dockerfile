FROM ubuntu:19.04 as builder

RUN apt-get update -qq && apt-get install -y \
    git cmake build-essential ninja-build binutils-dev libcurl4-openssl-dev zlib1g-dev libdw-dev libiberty-dev python3 \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/SimonKagstrom/kcov.git

WORKDIR /kcov

RUN mkdir build && \
    cd build && \
    cmake -G 'Ninja' .. && \
    cmake --build . && \
    cmake --build . --target install

FROM ubuntu:19.04

COPY --from=builder /usr/local/bin/kcov* /usr/local/bin/
COPY --from=builder /usr/local/share/doc/kcov /usr/local/share/doc/kcov

RUN apt-get update -qq && apt-get install -y \
    libssl-dev \
    binutils-dev \
    libcurl4 \
    libdw1 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

CMD ["/usr/local/bin/kcov"]
