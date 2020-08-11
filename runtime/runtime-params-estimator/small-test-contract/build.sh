#!/bin/bash

# First, measure the size of the file without payload.
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/smallest_contract.wasm
