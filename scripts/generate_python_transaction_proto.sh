#!/usr/bin/env bash
PARENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null && pwd )"
protoc -I=${PARENT_DIR}/protos --python_out=${PARENT_DIR}/scripts ${PARENT_DIR}/protos/protos/signed_transaction.proto
