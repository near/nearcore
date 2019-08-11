#!/bin/bash

cargo +nightly build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/test_contract_rs.wasm ../res/
rm -rf target
