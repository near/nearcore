#!/bin/bash
set -e

IMAGE=${1:-throwawaydude/alphanet:0.0.1}
PREFIX=${2:-alphanet}

echo "Starting 4 nodes prefixed $PREFIX of $IMAGE on GCloud..."

gcloud compute instances create-with-container $PREFIX-0 \
    --container-env BOOT_NODE_IP=127.0.0.1 \
    --container-env NODE_NUM=0 \
    --container-env TOTAL_NODES=4 \
    --container-image $IMAGE \
    --zone us-west2-a

BOOT_NODE_IP=$(
gcloud compute instances describe alphanet-0 \
    --zone us-west2-a | grep natIP | \
    awk '{print $2}'
)

gcloud compute instances create-with-container $PREFIX-1 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=1 \
    --container-env TOTAL_NODES=4 \
    --container-image $IMAGE \
    --zone us-west2-a
gcloud compute instances create-with-container $PREFIX-2 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=2 \
    --container-env TOTAL_NODES=4 \
    --container-image $IMAGE \
    --zone us-west2-a
gcloud compute instances create-with-container $PREFIX-3 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=3 \
    --container-env TOTAL_NODES=4 \
    --container-image $IMAGE \
    --zone us-west2-a
