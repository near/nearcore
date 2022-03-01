# Loadtest 

This test consists of two parts:

./target/debug/neard --home ~/.near_tmp/7 init --chain-id localnet --num-shards=5



./setup.sh / setup.py -- is creating the test (compiling the contract, creating accounts and deploying it).

./loadtest.py is the test itself, that tries to schedule as many transactions  as possible.