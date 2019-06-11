#!/bin/bash
set -ex

# Must start binary outside of this script.
./scripts/waitonserver.sh
#./scripts/build_wasm.sh

# Run nearlib tests
rm -rf nearlib
git clone --single-branch --branch nightshade https://github.com/nearprotocol/nearlib.git nearlib
cd nearlib
export NEAR_PROTOS_DIR="../nearcore/core/protos/protos"
export HELLO_WASM_PATH="../nearcore/tests/hello.wasm"
npm install
npm test
npm run build
npm run doc
cd ..

# Try creating and building new project using NEAR CLI tools
<<COMMENT
git clone https://git@github.com/nearprotocol/near-shell.git near-shell
git checkout origin/nightshade
cd near-shell
npm install
npm test
cd ..
COMMENT

./scripts/kill_near.sh
