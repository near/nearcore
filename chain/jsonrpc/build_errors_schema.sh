#!/bin/bash
cargo build --features dump_errors_schema
cp ../../target/rpc_errors_schema.json ./res/rpc_errors_schema.json
