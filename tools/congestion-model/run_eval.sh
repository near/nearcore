#!/bin/bash

# exit on error
set -euo pipefail

# show each executed command
# set -x

NUM_SHARDS=6
WARMUP=500
ROUNDS=1500
TX_POOL_SIZE=200

OPTIONS_STRING="--rounds ${ROUNDS} --warmup ${WARMUP} --shards ${NUM_SHARDS} --tx-pool-size ${TX_POOL_SIZE}"

echo "# Delay VS Utilization"
cargo run -q --release -- --workload RelayedHot --strategy SmoothTrafficLight ${OPTIONS_STRING} 
cargo run -q --release -- --workload RelayedHot --strategy NoQueues ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload RelayedHot --strategy STL_MAX_UTIL ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload RelayedHot --strategy STL_LOW_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload RelayedHot --strategy STL_MIN_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload RelayedHot --strategy NEPv3 ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload RelayedHot --strategy FancyStop ${OPTIONS_STRING} | tail -n 1

echo "# Big queues VS Utilization"
cargo run -q --release -- --workload BigLinearImbalance --strategy SmoothTrafficLight ${OPTIONS_STRING} 
cargo run -q --release -- --workload BigLinearImbalance --strategy NoQueues ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload BigLinearImbalance --strategy STL_MAX_UTIL ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload BigLinearImbalance --strategy STL_LOW_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload BigLinearImbalance --strategy STL_MIN_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload BigLinearImbalance --strategy NEPv3 ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload BigLinearImbalance --strategy FancyStop ${OPTIONS_STRING} | tail -n 1

echo "# Simple Utilization"
cargo run -q --release -- --workload Balanced --strategy SmoothTrafficLight ${OPTIONS_STRING} 
cargo run -q --release -- --workload Balanced --strategy NoQueues ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload Balanced --strategy STL_MAX_UTIL ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload Balanced --strategy STL_LOW_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload Balanced --strategy STL_MIN_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload Balanced --strategy NEPv3 ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload Balanced --strategy FancyStop ${OPTIONS_STRING} | tail -n 1

echo "# Fairness and Prioritization opportunity"
cargo run -q --release -- --workload FairnessTest --strategy SmoothTrafficLight ${OPTIONS_STRING} 
cargo run -q --release -- --workload FairnessTest --strategy NoQueues ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload FairnessTest --strategy STL_MAX_UTIL ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload FairnessTest --strategy STL_LOW_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload FairnessTest --strategy STL_MIN_DELAY ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload FairnessTest --strategy NEPv3 ${OPTIONS_STRING} | tail -n 1
cargo run -q --release -- --workload FairnessTest --strategy FancyStop ${OPTIONS_STRING} | tail -n 1


# Use this code to run all combos. But I usually find it better to control each group separately.
#
# STRATEGIES=("SmoothTrafficLight" "NoQueues" "STL_MAX_UTIL" "STL_LOW_DELAY" "STL_MIN_DELAY" "NEPv3" "FancyStop")
# WORKLOADS=("RelayedHot" "Balanced" "BigLinearImbalance" "FairnessTest")
#
# for WORKLOAD in "${WORKLOADS[@]}"
# do
#     for STRATEGY in "${STRATEGIES[@]}"
#     do
#         cargo run -q --release -- --workload ${WORKLOAD} --strategy ${STRATEGY} ${OPTIONS_STRING} | tail -n 1
#     done;
# done
