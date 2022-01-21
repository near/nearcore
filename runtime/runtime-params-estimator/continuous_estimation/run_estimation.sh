#!/bin/bash

if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

GIT_HASH=`git rev-parse HEAD`

cargo run --release -p runtime-params-estimator --features required -- \
    --vm-kind wasmer2 \
    --metric icount --docker --full \
    tee >(tr -d _ | awk -v git_hash="${GIT_HASH}" -f insert_icount.awk | sqlite3 ${SQLI_DB})