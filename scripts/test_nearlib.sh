#!/bin/bash
set -ex

./scripts/start_near.sh &
NEAR_PID=$!
trap 'kill $NEAR_PID' 0

# Must start binary outside of this script.
./scripts/waitonserver.sh
#./scripts/build_wasm.sh

# Run nearlib tests
rm -rf nearlib
git clone --single-branch --branch nightshade https://github.com/nearprotocol/nearlib.git nearlib
cd nearlib
export NEAR_PROTOS_DIR="../core/protos/protos"
export HELLO_WASM_PATH="../tests/hello.wasm"
npm install
npm run build
npm test
npm run doc
cd ..

# Try creating and building new project using NEAR CLI tools
git clone --single-branch --branch nightshade https://git@github.com/nearprotocol/near-shell.git near-shell
cd near-shell
npm install
#npm test
cd ..