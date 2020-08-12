#!/bin/bash

set -ex

VMKIND="wasmer"
echo $1
if [ "$1" == "wasmtime" ]; then
  VMKIND="$1";
fi



cargo build --release --package runtime-params-estimator
./emu-cost/counter_plugin/qemu-x86_64 -cpu Westmere-v1 -plugin file=./emu-cost/counter_plugin/libcounter.so ../../target/release/runtime-params-estimator --compile-only --vm-kind "$VMKIND"
