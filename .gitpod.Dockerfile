FROM gitpod/workspace-full

RUN git clone https://github.com/nearprotocol/nearcore.git --depth 1 /nearcore
RUN cd /nearcore && cargo build
