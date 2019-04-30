#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/nearcore:0.1.5}
PREFIX=${2:-testnet-${USER}}
STUDIO_IMAGE=${3:-nearprotocol/studio:0.2.4}
ZONE=${4:-us-west2-a}
REGION=${5:-us-west2}
NUM_NODES=${6:-10}
NUM_ACCOUNTS=${7:-400}

echo "Starting ${NUM_NODES} nodes prefixed ${PREFIX} of ${IMAGE} on GCloud ${ZONE} zone..."

set +e
gcloud compute firewall-rules describe nearmint-instance > /dev/null 2>&1
INSTANCE_FIRE_WALL_EXISTS=$?
gcloud beta compute addresses describe ${PREFIX}-0 --region ${REGION} > /dev/null 2>&1
ADDRESS_EXISTS=$?
gcloud beta compute instances describe ${PREFIX}-0 --zone ${ZONE} > /dev/null 2>&1
BOOTNODE_EXISTS=$?
set -e

if [[ ! ${INSTANCE_FIRE_WALL_EXISTS} -eq 0 ]]; then
gcloud compute firewall-rules create nearmint-instance \
    --allow tcp:26656,tcp:3030 \
    --target-tags=nearmint-instance
fi

if [[ ! ${ADDRESS_EXISTS} -eq 0 ]]; then
gcloud beta compute addresses create ${PREFIX}-0 --region ${REGION}
fi

if [[ ! ${BOOTNODE_EXISTS} -eq 0 ]]; then
gcloud beta compute instances create-with-container ${PREFIX}-0 \
    --container-env NODE_ID=0 \
    --container-env TOTAL_NODES=${NUM_NODES} \
    --container-env NUM_ACCOUNTS=${NUM_ACCOUNTS} \
    --container-env NODE_KEY="53Mr7IhcJXu3019FX+Ra+VbxSQ5y2q+pknmM463jzoFzldWZb16dSYRxrhYrLRXe/UA0wR2zFy4c3fY5yDHjlA==" \
    --container-image ${IMAGE} \
    --zone ${ZONE} \
    --tags=nearmint-instance \
    --create-disk=name=${PREFIX}-persistent-0,auto-delete=yes \
    --container-mount-disk mount-path="/srv/near" \
    --boot-disk-size 200GB \
    --address ${PREFIX}-0 \
    --machine-type n1-highcpu-4
fi

BOOT_NODE_IP=$(
    gcloud beta compute addresses describe ${PREFIX}-0 --region ${REGION}  | head -n 1 | awk '{print $2}'
)
echo "Connect to boot node: 6f99d7b49a10fff319cd8bbbd13c3a964dcd0248@${BOOT_NODE_IP}"

for NODE_ID in $(seq 1 `expr $NUM_NODES - 1`)
do

    set +e
    gcloud beta compute instances describe ${PREFIX}-${NODE_ID} --zone ${ZONE} > /dev/null 2>&1
    NODE_EXISTS=$?
    set -e

    if [[ ! ${NODE_EXISTS} -eq 0 ]]; then
    gcloud beta compute instances create-with-container ${PREFIX}-${NODE_ID} \
        --container-env BOOT_NODES="6f99d7b49a10fff319cd8bbbd13c3a964dcd0248@${BOOT_NODE_IP}:26656" \
        --container-env NODE_ID=${NODE_ID} \
	    --container-env TOTAL_NODES=${NUM_NODES} \
        --container-env NUM_ACCOUNTS=${NUM_ACCOUNTS} \
        --container-image ${IMAGE} \
        --zone ${ZONE} \
        --tags=testnet-instance \
        --create-disk=name=${PREFIX}-persistent-${NODE_ID},auto-delete=yes \
        --container-mount-disk=mount-path="/srv/near" \
        --boot-disk-size 200GB \
        --machine-type n1-highcpu-4 &

    fi
done
wait

echo "RPCs of the nodes"
for NODE_IP in $(gcloud compute instances list --filter="name:${PREFIX}*" | grep "RUNNING" | awk '{print $5}')
do
  echo "\"${NODE_IP}:3030\","
done

set +e
gcloud compute firewall-rules describe testnet-studio > /dev/null 2>&1
STUDIO_FIRE_WALL_EXISTS=$?
gcloud compute disks describe ${PREFIX}-studio-persistent  --zone ${ZONE} > /dev/null 2>&1
STUDIO_STORAGE_EXISTS=$?
gcloud beta compute instances describe ${PREFIX}-studio --zone ${ZONE} > /dev/null 2>&1
STUDIO_EXISTS=$?
gcloud beta compute addresses describe ${PREFIX}-studio --region ${REGION} > /dev/null 2>&1
STUDIO_ADDRESS_EXISTS=$?
set -e

if [[ ! ${STUDIO_FIRE_WALL_EXISTS} -eq 0 ]]; then
gcloud compute firewall-rules create testnet-studio \
    --allow tcp:80 \
    --target-tags=testnet-studio
fi

if [[ ! ${STUDIO_STORAGE_EXISTS} -eq 0 ]]; then
gcloud compute disks create --size 200GB --zone ${ZONE} \
    ${PREFIX}-studio-persistent
fi

if [[ ! ${STUDIO_ADDRESS_EXISTS} -eq 0 ]]; then
gcloud beta compute addresses create ${PREFIX}-studio --region ${REGION}
fi

if [[ !${STUDIO_EXISTS} -eq 0 ]]; then
gcloud beta compute instances create-with-container ${PREFIX}-studio \
    --container-env DEVNET_HOST=http://${BOOT_NODE_IP} \
    --container-env NEARLIB_COMMIT="348509b526cf4ca0495d86cb211d1013d84629a2" \
    --container-env NEARLIB_VERSION="0.5.2" \
    --container-env PLATFORM=GCP \
    --container-image ${STUDIO_IMAGE} \
    --zone ${ZONE} \
    --tags=testnet-studio \
    --disk=name=${PREFIX}-studio-persistent \
    --container-mount-disk=mount-path="/srv/near" \
    --boot-disk-size 200GB \
    --address ${PREFIX}-studio \
    --machine-type n1-standard-2
fi

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

echo "TestNet HTTP RPC interface is accessible at ${BOOT_NODE_IP}:3030"
echo "Waiting for studio instance to start. This could take a few minutes..."
wait_for_studio & spinner
echo "NEARStudio is now accessible at http://${STUDIO_IP}"

