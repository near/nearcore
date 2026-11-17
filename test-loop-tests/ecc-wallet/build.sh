#!/usr/bin/env bash
# cspell:ignore Ctarget Dwarnings Zbuild
# Builds the wallet contract and copies the wasm to `res/ecc_wallet.wasm`, which is checked in.
#
# The flags match `runtime/near-test-contracts/build.rs`: `-Ctarget-cpu=mvp` and a rebuilt std
# keep out wasm features the NEAR VM does not accept, such as bulk memory. The toolchain is the
# one nearcore's `rust-toolchain.toml` pins.
set -euo pipefail

cd "$(dirname "$0")"

RUSTC_BOOTSTRAP=1 RUSTFLAGS="-Dwarnings -Ctarget-cpu=mvp" cargo build \
  -Zbuild-std=panic_abort,std \
  --target=wasm32-unknown-unknown \
  --release \
  --target-dir target

install -D -m 644 target/wasm32-unknown-unknown/release/ecc_wallet.wasm res/ecc_wallet.wasm
