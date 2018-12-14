#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
DOCKER_BUILDKIT=1 docker build --build-arg PACKAGE=$1 -f ${DIR}/Dockerfile -t $2 ${DIR}/..
