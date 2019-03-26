#!/bin/bash
set -ex

./target/release/devnet &
./scripts/waitonserver.sh
./scripts/build_wasm.sh
cd nearlib && npm install && npm run build && npm run doc && npm test && cd ..
mkdir new_project && cd new_project && npm install near-shell && node_modules/near-shell/near new_project && npm install && npm run-script build && npm test && cd ..
./scripts/kill_devnet.sh