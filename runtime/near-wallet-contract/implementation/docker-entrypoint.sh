#!/bin/bash
RUSTFLAGS='-C link-arg=-s' cargo build --release --target wasm32-unknown-unknown
