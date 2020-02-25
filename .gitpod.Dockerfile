FROM gitpod/workspace-full

RUN rustup default nightly
RUN git clone https://github.com/nearprotocol/nearcore.git --depth 1 /nearcore
RUN cd /nearcore && cargo build