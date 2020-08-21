#!/bin/bash
rm -rf /Users/mike/.near/local
cargo build -p neard --release
./target/release/neard --home=/Users/mike/.near/local init
node mike-hack.js
cp /Users/mike/.near/local/validator_key.json /Users/mike/near/balancer-core/neardev/local/test.near.json
./target/release/neard --home=/Users/mike/.near/local run