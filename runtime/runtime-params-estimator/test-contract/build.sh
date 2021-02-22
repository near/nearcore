#!/bin/bash

# First, measure the size of the file without payload.
rm -rf target
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release
# bare_wasm=$(stat -c%s target/wasm32-unknown-unknown/release/test_contract.wasm)
bare_wasm=$(stat -f%z target/wasm32-unknown-unknown/release/test_contract.wasm)
echo ${bare_wasm}

# Generate several files of various sizes. We will compile these files into the Wasm binary to
# bloat its size to the given values.
# 10KiB
dd if=/dev/urandom of=./res/small_payload bs=$(expr 10240 - ${bare_wasm}) count=1
# 100KiB
dd if=/dev/urandom of=./res/medium_payload bs=$(expr 102400 - ${bare_wasm}) count=1
# 1MiB
dd if=/dev/urandom of=./res/large_payload bs=$(expr 1048576 - ${bare_wasm}) count=1

RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release  --features small_payload
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/small_contract.wasm
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release  --features medium_payload
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/medium_contract.wasm
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release  --features large_payload
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/large_contract.wasm

# Compiling nightly

rm -rf target
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release --features nightly_protocol_features
# bare_wasm=$(stat -c%s target/wasm32-unknown-unknown/release/test_contract.wasm)
bare_wasm=$(stat -f%z target/wasm32-unknown-unknown/release/test_contract.wasm)
echo ${bare_wasm}

# Generate several files of various sizes. We will compile these files into the Wasm binary to
# bloat its size to the given values.
# 10KiB
dd if=/dev/urandom of=./res/small_payload bs=$(expr 10240 - ${bare_wasm}) count=1
# 100KiB
dd if=/dev/urandom of=./res/medium_payload bs=$(expr 102400 - ${bare_wasm}) count=1
# 1MiB
dd if=/dev/urandom of=./res/large_payload bs=$(expr 1048576 - ${bare_wasm}) count=1

RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release  --features small_payload,nightly_protocol_features
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_small_contract.wasm
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release  --features medium_payload,nightly_protocol_features
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_medium_contract.wasm
RUSTFLAGS='-C link-arg=-s' cargo +nightly build --target wasm32-unknown-unknown --release  --features large_payload,nightly_protocol_features
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_large_contract.wasm
