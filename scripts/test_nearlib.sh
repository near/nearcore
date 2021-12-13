#!/usr/bin/env bash
set -ex

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

# Disabling yarn test for now
yarn test
yarn doc

