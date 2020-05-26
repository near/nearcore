#!/bin/bash
set -euo pipefail

branch=${BUILDKITE_BRANCH:-${GITHUB_REF##*/}}
commit=${BUILDKITE_COMMIT:-${GITHUB_SHA}}
if [[ ${commit} == "HEAD" ]]; then
    commit=$(git rev-parse HEAD)
fi
os=$(uname)

make release

function upload_binary {
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/$1
    aws s3 cp --acl public-read target/release/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/$1
}

function upload_debug_binary {
    aws s3 cp --acl public-read target/debug/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/$1
    aws s3 cp --acl public-read target/debug/$1 s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/$1
}

upload_binary near
upload_debug_binary keypair-generator
upload_debug_binary genesis-csv-to-json
upload_binary near-vm-runner-standalone
upload_debug_binary state-viewer
upload_debug_binary store-validator-bin
