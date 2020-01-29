#!/bin/bash
set -ex

export RUST_BACKTRACE=full
./scripts/start_unittest.py --local &
export NEAR_PID=$!
trap 'pkill -15 -P $NEAR_PID' 0

#./scripts/build_wasm.sh

function get_nearlib_nearshell_release () {
    rm -rf nearlib_release_test near-shell nearlib
    mkdir nearlib_release_test
    cd nearlib_release_test

    yarn add nearlib near-shell

    mv node_modules/nearlib ..
    mv node_modules/near-shell ..
    cd ..
}

function get_nearlib_nearshell_git () {
    rm -rf nearlib
    git clone https://github.com/nearprotocol/nearlib.git nearlib
}

if [ -z "${NEARLIB_RELEASE}" ]; then
    get_nearlib_nearshell_git
else
    get_nearlib_nearshell_release
fi

# Run nearlib tests
cd nearlib
yarn
yarn build
../scripts/waitonserver.sh
yarn test
yarn doc
