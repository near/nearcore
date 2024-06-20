#!/bin/bash

# Get arguments from the command line
TIME=$1
USERS=$2
SHARDS=$3
NODES=$4
RUMP_UP=$5

# Stop previous experiment
pkill -9 locust || true
nearup stop

# Build the project
make neard

# Start neard
nearup run localnet --binary-path target/release/ --num-nodes $NODES --num-shards $SHARDS --override

# Prepare python environment
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r pytest/requirements.txt
python -m pip install locust
export KEY=~/.near/localnet/node0/validator_key.json

# Run benchmark
cd pytest/tests/loadtest/locust/
nohup locust -H 127.0.0.1:3030 -f locustfiles/ft.py --funding-key=$KEY -t $TIME -u $USERS -r $RUMP_UP --processes 8 --headless &

# Give locust 5 minutes to start and rump up
sleep 300

# Run data collector
cd ~/nearcore
python3 scripts/ft-benchmark-data-sender.py

echo "Benchmark completed."
