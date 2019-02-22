#!/usr/bin/env bash
REPO_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../.." >/dev/null && pwd )"
protoc -I=${REPO_DIR}/protos --python_out=${REPO_DIR}/pynear/src/near/pynear ${REPO_DIR}/protos/protos/signed_transaction.proto
