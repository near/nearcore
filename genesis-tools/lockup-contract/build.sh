#!/bin/bash

RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/lockup_contract.wasm ./
#rm -rf target
md5sum ./res/lockup_contract.wasm > md5.txt
