#!/usr/bin/env bash

RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/test_contract_rs.wasm ../res/
rm -rf target

RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release --features nightly_protocol_features
cp target/wasm32-unknown-unknown/release/test_contract_rs.wasm ../res/nightly_test_contract_rs.wasm
