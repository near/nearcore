FROM amazonlinux:2

RUN yum update -y && yum install -y \
    openssl-devel.x86_64 \
    && yum clean all && rm -rf /var/cache/yum
