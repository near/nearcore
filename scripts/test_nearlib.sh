#!/bin/bash
set -ex

./target/release/devnet &
./scripts/waitonserver.sh
./scripts/build_wasm.sh

# Run nearlib tests
cd nearlib
npm install
npm run build
npm run doc
npm test
cd ..

# Try creating and building new project using NEAR CLI tools
rm -rf new_project
mkdir new_project
cd new_project
npm install git+https://git@github.com/nearprotocol/near-shell.git
$(npm bin)/near new_project
# Disabled running create_account / test, as it's currently deploys to general devnet instead of local.
# $(npm bin)/near create_account --account_id=near-hello-devnet
npm install
npm run build
# npm test
cd ..

./scripts/kill_devnet.sh
