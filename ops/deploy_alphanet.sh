#!/bin/bash
set -e

IMAGE=${1:-throwawaydude/alphanet:0.0.3}
PREFIX=${2:-alphanet}
STUDIO_IMAGE=${3:-throwawaydude/studio:0.0.0}
ZONE=${4:-us-west2-a}

echo "Starting 4 nodes prefixed ${PREFIX} of ${IMAGE} on GCloud ${ZONE} zone..."

gcloud compute instances create-with-container ${PREFIX}-0 \
    --container-env BOOT_NODE_IP=127.0.0.1 \
    --container-env NODE_NUM=0 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE}

BOOT_NODE_IP=$(
gcloud compute instances describe ${PREFIX}-0 \
    --zone ${ZONE} | grep natIP | \
    awk '{print $2}'
)
echo "Connect to boot node: ${BOOT_NODE_IP}:3000/7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t"

gcloud compute instances create-with-container ${PREFIX}-1 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=1 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE}

gcloud compute instances create-with-container ${PREFIX}-2 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=2 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE}

gcloud compute instances create-with-container ${PREFIX}-3 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=3 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE}

gcloud compute instances create-with-container ${PREFIX}-studio \
    --container-env DEVNET_HOST=http://${BOOT_NODE_IP} \
    --container-env PLATFORM=GCP \
    --container-image ${STUDIO_IMAGE} \
    --zone ${ZONE}

