FROM parity/rust:nightly

WORKDIR /usr/src/near
COPY . .

RUN cargo build

