FROM gitpod/workspace-full

RUN git clone https://github.com/nearprotocol/nearcore.git --depth 1 /home/gitpod/nearcore
RUN bash -cl "cd /home/gitpod/nearcore && cargo build && cargo test"
