#!/bin/bash

cargo +nightly build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/test_contract.wasm ../res/
rm -rf target
