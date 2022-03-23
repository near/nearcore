#!/bin/bash
set -e

SOURCE_REPO_HOME=`git rev-parse --show-toplevel`

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

# Build warehouse binary
pushd "${SOURCE_REPO_HOME}"
cargo build --release
# Some users have CARGO_TARGET_DIR pointing to a custom target directory
if [[ ${CARGO_TARGET_DIR} ]]; then
  WAREHOUSE_BINARY="${CARGO_TARGET_DIR}/release/estimator-warehouse"
else
  WAREHOUSE_BINARY="`git rev-parse --show-toplevel`/target/release/estimator-warehouse"
fi
popd

# Ensure full optimization
export CARGO_PROFILE_RELEASE_LTO=fat
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1

# Rebuild test contract
pushd "${SOURCE_REPO_HOME}/runtime/runtime-params-estimator/test-contract"
bash build.sh
popd

# Build estimator
# Again, some users have CARGO_TARGET_DIR pointing to a custom target directory
if [[ ${CARGO_TARGET_DIR} ]]; then
  ESTIMATOR_BINARY="${CARGO_TARGET_DIR}/release/runtime-params-estimator"
else
  ESTIMATOR_BINARY="`git rev-parse --show-toplevel`/target/release/runtime-params-estimator"
fi
cargo build --release -p runtime-params-estimator --features required;


#  Actual estimations
GIT_HASH=`git rev-parse HEAD`
COMMON_ARGS="--iters 5 --warmup-iters 1 --json-output"
# This has problems in qemu, leading to OOM
USE_LARGE_STATE="--accounts-num 1000000 --home ${ESTIMATOR_NEAR_HOME}"
# Options that are only relevant for time based estimations
TIME_OPTIONS="--drop-os-cache"
# Options that are only relevant for icount based estimations
ICOUNT_OPTIONS="--docker --full "

# Run time measurements outside docker and with sudo if possible.
if [ "$EUID" -ne 0 ]; then
  echo "Running as non-root, storage related costs might be inaccurate because OS caches cannot be dropped"
  TIME_OPTIONS=""
fi

IMPORT_OPTIONS="--db ${SQLI_DB} import --commit-hash=${GIT_HASH} "

"${ESTIMATOR_BINARY}" ${COMMON_ARGS} ${USE_LARGE_STATE} ${TIME_OPTIONS} --metric time \
    | "${WAREHOUSE_BINARY}" ${IMPORT_OPTIONS};

# Run icount measurements inside docker container.
"${ESTIMATOR_BINARY}" ${COMMON_ARGS} ${ICOUNT_OPTIONS} --metric icount \
    | "${WAREHOUSE_BINARY}" ${IMPORT_OPTIONS};
