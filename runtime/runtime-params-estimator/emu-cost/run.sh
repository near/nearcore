#!/usr/bin/env bash
cd "${0%/*}"

NEARCORE=$PWD/../../../

exec podman --runtime=crun run --rm \
     --mount type=bind,source=$HOST_DIR,target=/host \
     --mount type=bind,source=$NEARCORE,target=/host/nearcore \
     --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
     --network host \
     -i -t rust-emu \
     /usr/bin/env bash
