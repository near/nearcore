#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/nearcore:0.1.3}
PREFIX=${2:-testnet}
ZONE=${3:-us-west2-a}
NUM_NODES=${4:-2}

for NODE_ID in $(seq 0 `expr $NUM_NODES - 1`)
do

gcloud beta compute instances update-container ${PREFIX}-${NODE_ID} \
    --zone ${ZONE} \
    --container-image ${IMAGE}

done

