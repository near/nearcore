#!/usr/bin/env bash

./setup.sh

VMKIND="wasmer"
features="required"

if [ "$1" == "wasmtime" ]; then
  VMKIND="$1";
  features="$features"
fi

set -ex

cargo build --release --package runtime-params-estimator --features $features
./emu-cost/counter_plugin/qemu-x86_64 -cpu Haswell-v4 -plugin file=./emu-cost/counter_plugin/libcounter.so ../../target/release/runtime-params-estimator --compile-only --vm-kind "$VMKIND"
