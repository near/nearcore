#!/bin/bash
cargo build --features dump_errors_schema --manifest-path near/Cargo.toml
cp ../target/rpc_errors_schema.json ../res/rpc_errors_schema.json
