#!/bin/bash
set -euo pipefail

branch=${BUILDKITE_BRANCH}
commit=${BUILDKITE_COMMIT}
os=$(uname)

make release

function upload_binary {
    aws s3 cp --acl public-read target/release/near s3://build.nearprotocol.com/nearcore/${os}/${branch}/$1
    aws s3 cp --acl public-read target/release/near s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/$1
}

upload_binary near
upload_binary keypair-generator
upload_binary genesis-csv-to-json
upload_binary near-vm-runner-standalone
upload_binary state-viewer
