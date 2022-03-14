#!/bin/bash
set -e

SCRIPT_HOME=`pwd`

if [[ -z "${SQLI_DB}" ]]; then
  echo '$SQLI_DB is not defined, please set it as environment variable before running this script.'
  exit
fi

if [[ -z "${ESTIMATOR_NEAR_HOME}" ]]; then
  echo '$ESTIMATOR_NEAR_HOME is not defined, will use /tmp/estimator_home as near home'
  ESTIMATOR_NEAR_HOME="/tmp/estimator_home"
fi

if [[ -z "${REPO_UNDER_TEST}" ]]; then
  echo '$REPO_UNDER_TEST is not defined, will use repository that the script is running from.'
else
  echo "Estimating repository at ${REPO_UNDER_TEST}"
  cd $REPO_UNDER_TEST
fi

if [[ -z "${CARGO_TARGET_DIR}" ]]; then
  CARGO_TARGET_DIR="`git rev-parse --show-toplevel`/target"
fi

GIT_HASH=`git rev-parse HEAD`

COMMON_ARGS="--iters 5 --warmup-iters 1"
# This has problems in qemu, leading to OOM
USE_LARGE_STATE="--accounts-num 1000000 --home ${ESTIMATOR_NEAR_HOME}"
# Options that are only relevant for time based estimations
TIME_OPTIONS="--drop-os-cache"
# Options that are only relevant for icount based estimations
ICOUNT_OPTIONS="--docker --full "

# Ensure full optimization
export CARGO_PROFILE_RELEASE_LTO=fat
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

# Run icount measurments inside docker container. Output will be in stdout.
cargo run --release -p runtime-params-estimator --features required -- \
    ${COMMON_ARGS} ${ICOUNT_OPTIONS} \
    --metric icount \
    | tee >(while read LINE; do echo $LINE | tr -d _ | awk -v git_hash="${GIT_HASH}" -f ${SCRIPT_HOME}/insert_icount.awk | sqlite3 ${SQLI_DB}; done);
    
# Run time measurements outside docker and with sudo. Output is in stderr.
cargo build --release -p runtime-params-estimator --features required;
sudo "${CARGO_TARGET_DIR}/release/runtime-params-estimator" \
    ${COMMON_ARGS} ${USE_LARGE_STATE} ${TIME_OPTIONS} \
    --metric time \
    2> >(tee /dev/stderr | while read LINE; do echo $LINE | tr -d _ | awk -v git_hash="${GIT_HASH}" -f ${SCRIPT_HOME}/insert_time.awk | sqlite3 ${SQLI_DB}; done);
