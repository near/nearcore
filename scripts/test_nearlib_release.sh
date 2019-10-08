#!/bin/bash
set -ex

./scripts/start_unittest.py --local &
export NEAR_PID=$!
trap 'pkill -15 -P $NEAR_PID' 0

#./scripts/build_wasm.sh

# Run nearlib tests
rm -rf nearlib_release_test near-shell nearlib
mkdir nearlib_release_test
cd nearlib_release_test

yarn add nearlib near-shell

mv node_modules/nearlib ..
mv node_modules/near-shell ..

cd ../nearlib

yarn
yarn build
../scripts/waitonserver.sh
export HELLO_WASM_PATH=../tests/hello.wasm # Need delete after new near-hello based nearlib release
yarn test
yarn doc

# Try creating and building new project using NEAR CLI tools
cd ../near-shell
yarn
cd ..
