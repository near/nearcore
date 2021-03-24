#!/usr/bin/env bash

cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/empty_contract_rs.wasm target/release/
