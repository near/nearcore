#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/alphanet:0.1.6}
PREFIX=${2:-alphanet}
ZONE=${3:-us-west2-a}

gcloud beta compute instances update-container ${PREFIX}-0 \
    --zone ${ZONE} \
    --container-image ${IMAGE}

gcloud beta compute instances update-container ${PREFIX}-1 \
    --zone ${ZONE} \
    --container-image ${IMAGE}

gcloud beta compute instances update-container ${PREFIX}-2 \
    --zone ${ZONE} \
    --container-image ${IMAGE}

gcloud beta compute instances update-container ${PREFIX}-3 \
    --zone ${ZONE} \
    --container-image ${IMAGE}
