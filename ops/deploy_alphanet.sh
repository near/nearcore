#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/alphanet:0.1.6}
PREFIX=${2:-alphanet}
STUDIO_IMAGE=${3:-nearprotocol/studio:0.1.8}
ZONE=${4:-us-west2-a}
REGION=${5:-us-west2}

echo "Starting 4 nodes prefixed ${PREFIX} of ${IMAGE} on GCloud ${ZONE} zone..."

set +e
gcloud compute firewall-rules describe alphanet-instance > /dev/null 2>&1
INSTANCE_FIRE_WALL_EXISTS=$?
set -e

if [[ ! ${INSTANCE_FIRE_WALL_EXISTS} -eq 0 ]]; then
gcloud compute firewall-rules create alphanet-instance \
    --allow tcp:3000,tcp:3030 \
    --target-tags=alphanet-instance
fi

gcloud compute disks create --size 200GB --zone ${ZONE} \
    ${PREFIX}-persistent-0 \
    ${PREFIX}-persistent-1 \
    ${PREFIX}-persistent-2 \
    ${PREFIX}-persistent-3

gcloud beta compute addresses create ${PREFIX}-0 --region ${REGION}

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
    --address ${PREFIX}-0 \
    --machine-type n1-highcpu-4

BOOT_NODE_IP=$(
    gcloud beta compute addresses describe ${PREFIX}-0 --region ${REGION}  | head -n 1 | awk '{print $2}'
)
echo "Connect to boot node: ${BOOT_NODE_IP}:3000/7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t"

gcloud beta compute instances create-with-container ${PREFIX}-1 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=1 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-instance \
    --disk=name=${PREFIX}-persistent-1 \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB \
    --machine-type n1-highcpu-4

gcloud beta compute instances create-with-container ${PREFIX}-2 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=2 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-instance \
    --disk=name=${PREFIX}-persistent-2 \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB \
    --machine-type n1-highcpu-4

gcloud beta compute instances create-with-container ${PREFIX}-3 \
    --container-env BOOT_NODE_IP=${BOOT_NODE_IP} \
    --container-env NODE_NUM=3 \
    --container-env TOTAL_NODES=4 \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-instance \
    --disk=name=${PREFIX}-persistent-3 \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB \
    --machine-type n1-highcpu-4

set +e
gcloud compute firewall-rules describe alphanet-studio > /dev/null 2>&1
STUDIO_FIRE_WALL_EXISTS=$?
set -e

if [[ ! ${STUDIO_FIRE_WALL_EXISTS} -eq 0 ]]; then
gcloud compute firewall-rules create alphanet-studio \
    --allow tcp:80 \
    --target-tags=alphanet-studio
fi

gcloud compute disks create --size 200GB --zone ${ZONE} \
    ${PREFIX}-studio-persistent

gcloud beta compute instances create-with-container ${PREFIX}-studio \
    --container-env DEVNET_HOST=http://${BOOT_NODE_IP} \
    --container-env PLATFORM=GCP \
    --container-image ${STUDIO_IMAGE} \
    --zone ${ZONE} \
    --tags=alphanet-studio \
    --disk=name=${PREFIX}-studio-persistent \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB \
    --machine-type n1-standard-2

# borrowed from https://stackoverflow.com/a/20369590
spinner()
{
    local pid=$!
    local delay=0.75
    local spinstr='|/-\'
    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    printf "    \b\b\b\b"
}

STUDIO_IP=$(
gcloud compute instances describe ${PREFIX}-studio \
    --zone ${ZONE} | grep natIP | \
    awk '{print $2}'
)

wait_for_studio()
{
    while :
    do
        STATUS_CODE=$(curl -I ${STUDIO_IP} 2>/dev/null | head -n 1 | cut -d$' ' -f2);
        if [[ ${STATUS_CODE} -eq 200 ]]; then
            exit 0
        fi
        sleep 1
    done
}

echo "Alphanet HTTP interface is accessible at ${BOOT_NODE_IP}:3030"
echo "Waiting for studio instance to start. This could take a few minutes..."
wait_for_studio & spinner
echo "NEARStudio is now accessible at http://${STUDIO_IP}"
