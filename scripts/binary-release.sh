#!/bin/bash
set -euo pipefail

branch=${BUILDKITE_BRANCH}
commit=${BUILDKITE_COMMIT}
os=$(uname)

cargo build -p near --release
cargo build -p keypair-generator --release
cargo build -p genesis-csv-to-json --release

aws s3 cp --acl public-read target/release/near s3://build.nearprotocol.com/nearcore/${os}/${branch}/near
aws s3 cp --acl public-read target/release/near s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/near
aws s3 cp --acl public-read target/release/keypair-generator s3://build.nearprotocol.com/nearcore/${os}/${branch}/keypair-generator
aws s3 cp --acl public-read target/release/keypair-generator s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/keypair-generator
aws s3 cp --acl public-read target/release/genesis-csv-to-json s3://build.nearprotocol.com/nearcore/${os}/${branch}/genesis-csv-to-json
aws s3 cp --acl public-read target/release/genesis-csv-to-json s3://build.nearprotocol.com/nearcore/${os}/${branch}/${commit}/genesis-csv-to-json
