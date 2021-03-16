#!/bin/sh
docker run \
     --rm --mount type=bind,source=$HOST_DIR,target=/host \
     --cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
     -i -t rust-emu \
     /usr/bin/env bash
