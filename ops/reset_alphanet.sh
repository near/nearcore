#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/alphanet:0.1.6}
PREFIX=${2:-alphanet}
ZONE=${3:-us-west2-a}
REGION=${4:-us-west2}

gcloud beta compute instances stop --zone ${ZONE} \
    ${PREFIX}-0 \
    ${PREFIX}-1 \
    ${PREFIX}-2 \
    ${PREFIX}-3

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-0 \
    --remove-container-mounts mount-path="/srv/near"

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-1 \
    --remove-container-mounts mount-path="/srv/near"

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-2 \
    --remove-container-mounts mount-path="/srv/near"

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-3 \
    --remove-container-mounts mount-path="/srv/near"

gcloud beta compute instances detach-disk --zone ${ZONE} ${PREFIX}-0 \
    --disk ${PREFIX}-persistent-0

gcloud beta compute instances detach-disk --zone ${ZONE} ${PREFIX}-1 \
    --disk ${PREFIX}-persistent-1

gcloud beta compute instances detach-disk --zone ${ZONE} ${PREFIX}-2 \
    --disk ${PREFIX}-persistent-2

gcloud beta compute instances detach-disk --zone ${ZONE} ${PREFIX}-3 \
    --disk ${PREFIX}-persistent-3

gcloud compute disks delete --zone ${ZONE} -q \
    ${PREFIX}-persistent-0 \
    ${PREFIX}-persistent-1 \
    ${PREFIX}-persistent-2 \
    ${PREFIX}-persistent-3

gcloud compute disks create --size 200GB --zone ${ZONE} \
    ${PREFIX}-persistent-0 \
    ${PREFIX}-persistent-1 \
    ${PREFIX}-persistent-2 \
    ${PREFIX}-persistent-3

gcloud beta compute instances attach-disk --zone ${ZONE} ${PREFIX}-0 \
    --disk ${PREFIX}-persistent-0 \
    --device-name ${PREFIX}-persistent-0

gcloud beta compute instances attach-disk --zone ${ZONE} ${PREFIX}-1 \
    --disk ${PREFIX}-persistent-1 \
    --device-name ${PREFIX}-persistent-1

gcloud beta compute instances attach-disk --zone ${ZONE} ${PREFIX}-2 \
    --disk ${PREFIX}-persistent-2 \
    --device-name ${PREFIX}-persistent-2

gcloud beta compute instances attach-disk --zone ${ZONE} ${PREFIX}-3 \
    --disk ${PREFIX}-persistent-3 \
    --device-name ${PREFIX}-persistent-3

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-0 \
    --container-mount-disk name="${PREFIX}-persistent-0",mount-path="/srv/near"

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-1 \
    --container-mount-disk name="${PREFIX}-persistent-1",mount-path="/srv/near"

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-2 \
    --container-mount-disk name="${PREFIX}-persistent-2",mount-path="/srv/near"

gcloud beta compute instances update-container --zone ${ZONE} ${PREFIX}-3 \
    --container-mount-disk name="${PREFIX}-persistent-3",mount-path="/srv/near"

gcloud beta compute instances start --zone ${ZONE} \
    ${PREFIX}-0 \
    ${PREFIX}-1 \
    ${PREFIX}-2 \
    ${PREFIX}-3
