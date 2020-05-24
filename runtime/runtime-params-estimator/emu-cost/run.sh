#!/bin/sh
docker run \
     --mount type=bind,source=$HOST_DIR,target=/host \
     --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
     -i -t rust-emu \
     /bin/bash
