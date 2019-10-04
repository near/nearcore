#!/bin/bash
set -ex

./scripts/start_unittest.py --local &
export NEAR_PID=$!
trap 'pkill -15 -P $NEAR_PID' 0

#./scripts/build_wasm.sh

# Run nearlib tests
rm -rf nearlib
git clone --single-branch https://github.com/nearprotocol/nearlib.git nearlib
cd nearlib
yarn
yarn build
../scripts/waitonserver.sh
yarn test
yarn doc
cd ..

# Try creating and building new project using NEAR CLI tools
git clone --single-branch https://git@github.com/nearprotocol/near-shell.git near-shell
cd near-shell
yarn
#yarn test
cd ..
