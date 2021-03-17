#!/usr/bin/env bash

RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/tiny_contract_rs.wasm ./res/
rm -rf target
