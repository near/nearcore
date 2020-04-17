# our local base image
# FROM ubuntu
FROM rust

LABEL description="Container for builds"

# RUN rustup update nightly
RUN rustup toolchain uninstall nightly
# Must match nearcore/rust-toolchain.
RUN rustup default nightly-2020-03-19
RUN rustup target add wasm32-unknown-unknown

# install build dependencies for QEMU
RUN apt-get update && apt-get install -y  g++ rsync zip openssh-server \
     make apt-utils git sudo pkg-config libglib2.0-dev curl clang gdb

# Optional: configure SSH
# RUN mkdir -p /var/run/sshd

# RUN echo 'PasswordAuthentication yes' >> /etc/ssh/sshd_config && ssh-keygen -A

# RUN useradd -m -d /home/near -s /bin/bash -G sudo near
# RUN mkdir /home/near/.ssh && echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDo4/aJwbzM8hPXpyQ7x84iUHPwLz+YuoNg2/dtawYz5lZqV7lRUAYDC2+wIv3CKuZaXQ4NahNd8ajA2L2G/7i6uxME4442ybwgYPBaOTKO3VUDqZ5ZLzdr/rBKeyk7gw20FjyEFjbh1OYxKqFvTqUCHNdSEQ/G+nrQTJFPVlAIeOpV9tK13+rejYPnyGUreeqQsflv2fnXsjTSV1IppshDGf1pTCXyp+md48JLWfAE1J1Pl1iTErE1fzF09DJvufVaCuVw0DRvR1KE5ycz+/O6UPQjvd6mQxvHLkLyppT1v3G/RjyyTFO7e7u6O3gy5vEbXIJX4hF//NZrto+s3ghj igotti@iMac-Nikolay.local" > /home/near/.ssh/authorized_keys
# RUN service ssh start

# expose port 22
# EXPOSE 22