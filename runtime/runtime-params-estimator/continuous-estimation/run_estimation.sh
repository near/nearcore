#!/bin/bash

if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

GIT_HASH=`git rev-parse HEAD`

# Run icount measurments inside docker container. Output will be in stdout.
cargo run --release -p runtime-params-estimator --features required -- \
    --vm-kind wasmer2 \
    --metric icount --docker --full \
    | tee >(while read LINE; do echo $LINE | tr -d _ | awk -v git_hash="${GIT_HASH}" -f insert_icount.awk | sqlite3 ${SQLI_DB}; done);
    
# Run time measurements normally (outside docker). Output is in stderr.
cargo run --release -p runtime-params-estimator --features required -- \
    --vm-kind wasmer2 \
    --metric time \
    2> >(tee /dev/stderr | while read LINE; do echo $LINE | tr -d _ | awk -v git_hash="${GIT_HASH}" -f insert_time.awk | sqlite3 ${SQLI_DB}; done);
