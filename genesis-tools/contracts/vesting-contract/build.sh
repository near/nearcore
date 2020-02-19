#!/bin/bash

RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/vesting_contract.wasm ../res
#rm -rf target
md5sum ./*.wasm > md5.txt
