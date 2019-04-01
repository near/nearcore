#!/bin/bash
set -e

STUDIO_IMAGE=${3:-nearprotocol/studio:0.1.6}
PREFIX=${2:-alphanet}
ZONE=${3:-us-west2-a}

gcloud beta compute instances update-container ${PREFIX}-0 \
    --zone ${ZONE} \
    --container-image ${STUDIO_IMAGE}
