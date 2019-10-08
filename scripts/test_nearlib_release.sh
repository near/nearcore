#!/bin/bash
set -ex

./scripts/start_unittest.py --local &
export NEAR_PID=$!
trap 'pkill -15 -P $NEAR_PID' 0

#./scripts/build_wasm.sh

# Run nearlib tests
rm -rf nearlib_release_test
mkdir nearlib_release_test
cd nearlib_release_test

yarn add nearlib
rm package.json yarn.lock
mv node_modules/nearlib .
cd nearlib
yarn
yarn build
../../scripts/waitonserver.sh
yarn test
yarn doc
cd ..

# Try creating and building new project using NEAR CLI tools
yarn add near-shell
rm package.json yarn.lock
mv node_modules/near-shell .
ce near-shell
yarn
cd ../..