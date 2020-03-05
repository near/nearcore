FROM amazonlinux:2 as builder

RUN yum update -y && yum install -y \
    openssl-devel.x86_64 elfutils-libelf-devel libcurl-devel binutils-devel elfutils-devel zlib-devel \
    git gcc-c++ make ninja-build python3 tar wget \
    && yum clean all && rm -rf /var/cache/yum

RUN wget https://cmake.org/files/v3.10/cmake-3.10.0.tar.gz && \
    tar -xvzf cmake-3.10.0.tar.gz && \
    cd cmake-3.10.0 && \
    ./bootstrap && \
    make && \
    make install && \
    cd .. && \
    rm -rf cmake*

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
