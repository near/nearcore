#!/bin/bash
set -ex

# First, measure the size of the file without payload.
rm -rf target
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release
# bare_wasm=$(stat -c%s target/wasm32-unknown-unknown/release/test_contract.wasm)
bare_wasm=$(stat -f%z target/wasm32-unknown-unknown/release/test_contract.wasm)
echo ${bare_wasm}

# Generate several files of various sizes. We will compile these files into the Wasm binary to
# bloat its size to the given values.

# 10KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 10240 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features payload
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/stable_small_contract.wasm

# 100KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 102400 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features payload
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/stable_medium_contract.wasm

# 1MiB
dd if=/dev/urandom of=./res/payload bs=$(expr 1048576 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features payload
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/stable_large_contract.wasm

rm ./res/payload

# Compiling nightly

rm -rf target
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release --features nightly_protocol_features
# bare_wasm=$(stat -c%s target/wasm32-unknown-unknown/release/test_contract.wasm)
bare_wasm=$(stat -f%z target/wasm32-unknown-unknown/release/test_contract.wasm)
echo ${bare_wasm}

# Generate several files of various sizes. We will compile these files into the Wasm binary to
# bloat its size to the given values.

# Note the base is 16057 due to alt_bn128 hardcoded input.
# 20KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 20480 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features payload,nightly_protocol_features
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_small_contract.wasm

# 100KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 102400 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features payload,nightly_protocol_features
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_medium_contract.wasm

# 1MiB
dd if=/dev/urandom of=./res/payload bs=$(expr 1048576 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features payload,nightly_protocol_features
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_large_contract.wasm

rm ./res/payload
