#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/alphanet:0.1.2}
PREFIX=${2:-alphanet}
ZONE=${3:-us-west2-a}
REGION=${4:-us-west2}

gcloud compute instances delete --zone ${ZONE} --delete-disks=all -q \
    ${PREFIX}-0 \
    ${PREFIX}-1 \
    ${PREFIX}-2 \
    ${PREFIX}-3

gcloud compute disks create --size 200GB --zone ${ZONE} \
    ${PREFIX}-persistent-0 \
    ${PREFIX}-persistent-1 \
    ${PREFIX}-persistent-2 \
    ${PREFIX}-persistent-3

gcloud beta compute instances create-with-container ${PREFIX}-0 \
    --container-env BOOT_NODE_IP=127.0.0.1 \
    --container-env NODE_NUM=0 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-instance \
    --disk name=${PREFIX}-persistent-0 \
    --container-mount-disk mount-path="/srv/near" \
    --boot-disk-size 200GB \
    --address ${PREFIX}-0

BOOT_NODE_IP=$(
    gcloud beta compute addresses describe ${PREFIX}-0 --region ${REGION}  | head -n 1 | awk '{print $2}'
)

gcloud beta compute instances create-with-container ${PREFIX}-1 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=1 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-instance \
    --disk=name=${PREFIX}-persistent-1 \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB

gcloud beta compute instances create-with-container ${PREFIX}-2 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=2 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-instance \
    --disk=name=${PREFIX}-persistent-2 \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB

gcloud beta compute instances create-with-container ${PREFIX}-3 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=3 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-instance \
    --disk=name=${PREFIX}-persistent-3 \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB
