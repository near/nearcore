#!/bin/bash
sudo apt update
sudo apt install -y pkg-config libssl-dev build-essential cmake
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2019-05-22
source .cargo/env
git clone --single-branch --branch staging https://github.com/nearprotocol/nearcore.git nearcore
cd nearcore
cargo build -p near