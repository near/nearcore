#!/bin/bash

cd lockup-contract
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/lockup_contract.wasm ../res/
cd ../vesting-contract
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release
cp target/wasm32-unknown-unknown/release/vesting_contract.wasm ../res/
cd ..
#rm -rf target
# NOTE on Mac OS X install using `brew install md5sha1sum`
md5sum ./res/*.wasm > md5.txt
