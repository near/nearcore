#!/bin/sh

nearup stop
rm -rf $HOME/.near/localnet
nearup run localnet --binary-path $HOME/nearcore/target/release --num-nodes 1 --num-shards 1
sleep 5

export KEY=~/.near/localnet/node0/validator_key.json
#locust -H 127.0.0.1:3030  -f locustfiles/ft.py  --funding-key=$KEY --processes 4
#locust -H 127.0.0.1:3030  -f locustfiles/ft.py  --funding-key=$KEY --users 1000 --headless --run-time 300s --processes 2
locust -H 127.0.0.1:3030  -f locustfiles/ft.py  --funding-key=$KEY --users 500 --headless
