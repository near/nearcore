#!/usr/bin/env bash

./setup.sh

vmkind="wasmer"
features="required"

if [[ ! -z "$1" ]]; then
  features="$features,$1"
  if [[ "$1" == *"wasmtime"* || "$1" == *"lightbeam"* ]]; then
    vmkind="wasmtime";
  fi
fi

rm -rf /tmp/data

set -ex

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "${script_dir}"/../../
cargo run --release --package neard --bin neard -- --home /tmp/data init --test-seed=alice.near --account-id=test.near --fast
cd "${script_dir}"/../../genesis-tools/genesis-populate
cargo run --release --package genesis-populate --bin genesis-populate -- --additional-accounts-num=10000000 --home /tmp/data
cd "${script_dir}"
cargo build --release --package runtime-params-estimator --features $features
./emu-cost/counter_plugin/qemu-x86_64 -cpu Westmere-v1 -plugin file=./emu-cost/counter_plugin/libcounter.so ../../target/release/runtime-params-estimator --home /tmp/data --accounts-num 1000000 --iters 1 --warmup-iters 0 --warmup-transactions 0 --vm-kind $vmkind

