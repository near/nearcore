#!/bin/bash
set -e

gcloud compute instances create-with-container alphanet-0 \
    --container-env BOOT_NODE_IP=127.0.0.1 \
    --container-env NODE_NUM=0 \
    --container-env TOTAL_NODES=4 \
    --container-image throwawaydude/alphanet:0.0.1 \
    --zone us-west2-a

BOOT_NODE_IP=$(
gcloud compute instances describe alphanet-0 \
    --zone us-west2-a | grep natIP | \
    awk '{print $2}'
)

gcloud compute instances create-with-container alphanet-1 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=1 \
    --container-env TOTAL_NODES=4 \
    --container-image throwawaydude/alphanet:0.0.1 \
    --zone us-west2-a
gcloud compute instances create-with-container alphanet-2 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=2 \
    --container-env TOTAL_NODES=4 \
    --container-image throwawaydude/alphanet:0.0.1 \
    --zone us-west2-a
gcloud compute instances create-with-container alphanet-3 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=3 \
    --container-env TOTAL_NODES=4 \
    --container-image throwawaydude/alphanet:0.0.1 \
    --zone us-west2-a
