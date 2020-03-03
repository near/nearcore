FROM amazonlinux:2 as builder

RUN yum update -y && yum install -y \
    openssl-devel.x86_64 elfutils-libelf-devel libcurl-devel binutils-devel elfutils-devel zlib-devel git cmake3 ninja-build python3 \
    && yum clean all && rm -rf /var/cache/yum

RUN git clone https://github.com/SimonKagstrom/kcov.git

WORKDIR /kcov

RUN mkdir build && \
    cd build && \
    cmake -G 'Ninja' .. && \
    cmake --build . && \
    cmake --build . --target install

FROM amazonlinux:2

COPY --from=builder /usr/local/bin/kcov* /usr/local/bin/
COPY --from=builder /usr/local/share/doc/kcov /usr/local/share/doc/kcov

RUN yum update -y && yum install -y \
    openssl-devel.x86_64 elfutils-libelf-devel libcurl-devel binutils-devel elfutils-devel zlib-devel \
    && yum clean all && rm -rf /var/cache/yum

CMD ["/usr/local/bin/kcov"]
