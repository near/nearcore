#!/bin/bash

pushd $(dirname "$0")/to-wasm
cargo +nightly build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/to_wasm.wasm ../
wasm-gc ../to_wasm.wasm
popd

pushd $(dirname "$0")/import-memory
cargo +nightly run --release
wasm-gc ../../res/wasm_with_mem.wasm
popd

rm $(dirname "$0")/to_wasm.wasm