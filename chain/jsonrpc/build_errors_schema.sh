#!/usr/bin/env bash
cd "${0%/*}/../.." # ensure we're in the workspace directory
rm target/rpc_errors_schema.json
cargo build -p near-jsonrpc --features dump_errors_schema
cp target/rpc_errors_schema.json chain/jsonrpc/res/rpc_errors_schema.json
