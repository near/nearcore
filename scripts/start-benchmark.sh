#!/bin/bash

# Get arguments from the command line
TIME=$1
USERS=$2
SHARDS=$3
NODES=$4
RUMP_UP=$5
USER=$6
CONTEXT=$7

# cspell:words pkill locustfiles nohup venv

# Stop previous experiment
pkill -9 locust || true
nearup stop

# Build the project
make neard

# Clean data for the next run TODO: should be more robust for stateful runs
rm -rf ~/.near/localnet/node0/data

# Start neard
nearup run localnet --binary-path target/release/ --num-nodes $NODES --num-shards $SHARDS

# Prepare python environment
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r pytest/requirements.txt
python -m pip install locust
export KEY=~/.near/localnet/node0/validator_key.json

# Run benchmark
cd pytest/tests/loadtest/locust/
nohup locust -H 127.0.0.1:3030 -f locustfiles/ft.py --funding-key=$KEY -t "${TIME}s" -u $USERS -r $RUMP_UP --processes 8 --headless &

# Give locust 0.5 minutes to start and rump up
sleep 30

# Run data collector
cd ~/nearcore
python3 scripts/ft-benchmark-data-sender.py --duration $TIME --users $USERS --user $USER --context $CONTEXT

echo "Benchmark completed."
