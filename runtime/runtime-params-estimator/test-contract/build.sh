#!/usr/bin/env bash
set -ex

# Make sure we're executing from the directory with the contract (this script invokes
# $PWD-sensitive commands)
cd "$(dirname $0)"

features_with_arg=""
features_with_comma=""

if [[ ! -z "$1" ]]; then
  features_with_arg="--features $1"
  features_with_comma=",$1"
fi

function filesize
{
  local file=$1
  size=`stat -c%s $file 2>/dev/null` # linux
  if [ $? -eq 0 ]; then
    echo $size
    return 0
  fi

  size=`stat -f%z $file 2>/dev/null` # macos
  if [ $? -eq 0 ]; then
    echo $size
    return 0
  fi

  exit 1
}

rustup target add wasm32-unknown-unknown

# Some users have CARGO_TARGET_DIR pointing to a custom target directory. This
# script assumes a local target directory.
unset CARGO_TARGET_DIR;

# First, measure the size of the file without payload.
rm -rf target
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release $features_with_arg
bare_wasm=$(filesize target/wasm32-unknown-unknown/release/test_contract.wasm)
echo ${bare_wasm}

# Generate several files of various sizes. We will compile these files into the Wasm binary to
# bloat its size to the given values.

# 10KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 10240 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features "payload$features_with_comma"
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/stable_small_contract.wasm

# 100KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 102400 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features "payload$features_with_comma"
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/stable_medium_contract.wasm

# 1MiB
dd if=/dev/urandom of=./res/payload bs=$(expr 1048576 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features "payload$features_with_comma"
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/stable_large_contract.wasm

rm ./res/payload

# Compiling nightly

rm -rf target
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release --features "nightly$features_with_comma"
bare_wasm=$(filesize target/wasm32-unknown-unknown/release/test_contract.wasm)
echo ${bare_wasm}

# Generate several files of various sizes. We will compile these files into the Wasm binary to
# bloat its size to the given values.

# Note the base is 16057 due to alt_bn128 hardcoded input.
# 20KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 20480 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features "payload,nightly$features_with_comma"
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_small_contract.wasm

# 100KiB
dd if=/dev/urandom of=./res/payload bs=$(expr 102400 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features "payload,nightly$features_with_comma"
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_medium_contract.wasm

# 1MiB
dd if=/dev/urandom of=./res/payload bs=$(expr 1048576 - ${bare_wasm}) count=1
RUSTFLAGS='-C link-arg=-s' cargo build --target wasm32-unknown-unknown --release  --features "payload,nightly$features_with_comma"
cp target/wasm32-unknown-unknown/release/test_contract.wasm ./res/nightly_large_contract.wasm

rm ./res/payload
