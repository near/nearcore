#!/bin/bash

RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/lockup_contract.wasm ./
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release --features vesting
cp target/wasm32-unknown-unknown/release/lockup_contract.wasm ./lockup_vesting_contract.wasm
rm -rf target
md5sum ./*.wasm > md5.txt
