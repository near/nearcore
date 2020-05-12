#!/bin/bash
rm ../../target/rpc_errors_schema.json
cargo build --features dump_errors_schema
cp ../../target/rpc_errors_schema.json ./res/rpc_errors_schema.json
