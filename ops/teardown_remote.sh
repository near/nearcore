#!/bin/bash
set -e

PREFIX=${1:-testnet-${USER}}
NUM_NODES=${2:-50}
ZONE=${3:-us-west2-a}

for NODE_ID in $(seq 0 `expr $NUM_NODES - 1`)
do
    yes | gcloud beta compute instances delete ${PREFIX}-${NODE_ID} --zone=${ZONE} &
done
wait
