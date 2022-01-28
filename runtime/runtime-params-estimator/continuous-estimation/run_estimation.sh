#!/bin/bash

if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

if [[ -z "${ESTIMATOR_NEAR_HOME}" ]]; then
  echo '$ESTIMATOR_NEAR_HOME is not defined, will use /tmp/estimator_home as near home'
  ESTIMATOR_NEAR_HOME="/tmp/estimator_home"
  echo 'Initialize near home'
  cargo run --release --package neard --bin neard -- --home ${ESTIMATOR_NEAR_HOME} init --test-seed=alice.near --account-id=test.near --fast
  cargo run --release --package genesis-populate --bin genesis-populate -- --additional-accounts-num=10000000 --home ${ESTIMATOR_NEAR_HOME}
fi

GIT_HASH=`git rev-parse HEAD`

COMMON_ARGS="--vm-kind wasmer2 --home ${ESTIMATOR_NEAR_HOME} --accounts-num 1000000 --iters 1 --warmup-iters 1"

# Run icount measurments inside docker container. Output will be in stdout.
cargo run --release -p runtime-params-estimator --features required -- \
    ${COMMON_ARGS} \
    --metric icount --docker --full \
    | tee >(while read LINE; do echo $LINE | tr -d _ | awk -v git_hash="${GIT_HASH}" -f insert_icount.awk | sqlite3 ${SQLI_DB}; done);
    
# Run time measurements normally (outside docker). Output is in stderr.
cargo run --release -p runtime-params-estimator --features required -- \
    ${COMMON_ARGS} \
    --metric time \
    2> >(tee /dev/stderr | while read LINE; do echo $LINE | tr -d _ | awk -v git_hash="${GIT_HASH}" -f insert_time.awk | sqlite3 ${SQLI_DB}; done);
