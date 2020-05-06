#!/bin/bash
set -euo pipefail

branch=${BUILDKITE_BRANCH}
commit=${BUILDKITE_COMMIT}
if [[ ${commit} == "HEAD" ]]; then
    commit=$(git rev-parse HEAD)
fi

make
# Here we don't check master, beta and stable criteria, they have to be checked in buildkite pipeline
# before this script. 
docker tag nearcore nearprotocol/nearcore:${branch}-${commit}
docker tag nearcore nearprotocol/nearcore:${branch}

set -x
docker push nearprotocol/nearcore:${branch}-${commit}
docker push nearprotocol/nearcore:${branch}
if [[ ${branch} == "stable" ]]; then
    docker tag nearcore nearprotocol/nearcore:latest
    docker push nearprotocol/nearcore:latest
fi