#!/bin/bash
set -ex

export RUST_BACKTRACE=full
./scripts/start_unittest.py --local &
export NEAR_PID=$!
trap 'pkill -15 -P $NEAR_PID' 0

#./scripts/build_wasm.sh

rm -rf near-api-js near-shell create-near-app
git clone https://github.com/near/near-api-js.git
git clone https://github.com/near/near-shell.git
git clone https://github.com/near/create-near-app.git

# Make sure to use local nearcore for tests
export NODE_ENV=local

SRC_DIR=$(pwd)/$(dirname "${BASH_SOURCE[0]}")
export HOME=$SRC_DIR/../testdir

# Run near-api-js tests
cd near-api-js
yarn
yarn build
../scripts/waitonserver.sh
yarn test
yarn doc

# Run create-near-app tests
cd ../create-near-app
yarn 
yarn test

# Run near-shell tests
# cd ../near-shell
# yarn
# HOME=../testdir yarn test
