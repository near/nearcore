#!/usr/bin/env bash

# First, measure the size of the file without payload.
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/smallest_contract.wasm ./res/smallest_contract.wasm
