#!/bin/bash

./setup.sh

vmkind="wasmer"
features=""

if [ "$1" == "wasmtime" ]; then
  vmkind="$1";
  features="--features wasmtime"
fi
if [ "$1" == "lightbeam" ]; then
  vmkind="wasmtime"
  features="--features lightbeam"
fi



rm -rf /tmp/data

set -ex

cargo run --release --package neard --bin neard -- --home /tmp/data init --chain-id= --test-seed=alice.near --account-id=test.near --fast
cargo run --release --package genesis-populate --bin genesis-populate -- --additional-accounts-num=200000 --home /tmp/data
cargo build --release --package runtime-params-estimator $features
./emu-cost/counter_plugin/qemu-x86_64 -cpu Westmere-v1 -plugin file=./emu-cost/counter_plugin/libcounter.so ../../target/release/runtime-params-estimator --home /tmp/data --accounts-num 20000 --iters 1 --warmup-iters 1 

