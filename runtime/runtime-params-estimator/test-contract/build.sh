#!/usr/bin/env bash
set -ex

# Make sure we're executing from the directory with the contract (this script invokes
# $PWD-sensitive commands)
cd "$(dirname $0)"

features_with_arg=""
features_with_comma=""

if [[ ! -z "$1" ]]; then
  features_with_arg="--features $1"
  features_with_comma=",$1"
fi

rustup target add wasm32-unknown-unknown

# Some users have CARGO_TARGET_DIR pointing to a custom target directory. This
# script assumes a local target directory.
unset CARGO_TARGET_DIR;

# Build stable_contract.wasm
rm -rf target
cargo build --target wasm32-unknown-unknown --release $features_with_arg
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/stable_contract.wasm

# Build nightly_contract.wasm
rm -rf target
cargo build --target wasm32-unknown-unknown --release --features "nightly$features_with_comma"
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_contract.wasm
