#!/usr/bin/env bash
# cspell:ignore Ctarget Dwarnings Zbuild
# Builds the wallet contract on its own, for deploying it outside nearcore's tests. The tests
# don't need this: `runtime/near-test-contracts/build.rs` builds the wallet with the same flags.
#
# `-Ctarget-cpu=mvp` and a rebuilt std keep out wasm features the NEAR VM does not accept, such
# as bulk memory. The toolchain is the one nearcore's `rust-toolchain.toml` pins.
set -euo pipefail

cd "$(dirname "$0")"

RUSTC_BOOTSTRAP=1 RUSTFLAGS="-Dwarnings -Ctarget-cpu=mvp" cargo build \
  -Zbuild-std=panic_abort,std \
  --target=wasm32-unknown-unknown \
  --release \
  --locked \
  --target-dir target

echo "built target/wasm32-unknown-unknown/release/ecc_wallet.wasm"
