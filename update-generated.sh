#!/usr/bin/env bash
# Updates all generated artifacts checked into the repo.

set -eux

cd "$(dirname "${BASH_SOURCE[0]}")"

cargo run -p near-jsonrpc-openapi-spec > chain/jsonrpc/openapi/openapi.json

cargo insta test --accept -p near-parameters

set +e
cargo run -p protocol-schema-check
schema_status=$?
set -e

case "$schema_status" in
    0) ;;
    1) cp "${CARGO_TARGET_DIR:-target}/protocol_schema.toml" tools/protocol-schema-check/res/protocol_schema.toml ;;
    *) exit "$schema_status" ;;
esac
