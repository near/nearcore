#!/bin/bash
# Usage: create_instance_pool.sh machine_name zones_list near_home prefix
# machine_name: create instance to be machine_name-0, machine_name-1, ..., default: near-pytest
# zones_list: a list of zones to create instance in, can be repetitive, also determines number
#   of instance to create. Default: "us-west2-a us-west2-b us-west2-c"
# near_home: local location of NEAR_HOME. default: ~/.near
# prefix: local location of testnet config dir, default: test (so configs are ~/.near/test0, ...)
set -e

MACHINE_NAME=${1:-near-pytest}
ZONES_LIST=${2:-"us-west2-a us-west2-b us-west2-c"} # us-east1-b us-east1-c us-east1-d us-central1-a us-central1-b us-central1-c us-central1-f list more by `gcloud compute zones list`
NEAR_HOME=${3:-${HOME}/.near}
PREFIX=${4:-test}
ZONES=($ZONES_LIST)
NUM_NODES=${#ZONES[@]} 
ZONE=${ZONES[0]}

FIREWALL_TAG=${MACHINE_NAME}

# Create firewall rules, if necessary.
set +e
gcloud compute firewall-rules describe ${FIREWALL_TAG} > /dev/null 2>&1
FIREWALL_EXISTS=$?
set -e
if [[ ! ${FIREWALL_EXISTS} -eq 0 ]]; then
gcloud compute firewall-rules create ${FIREWALL_TAG} \
    --allow tcp:24567,tcp:3030 \
    --target-tags=${FIREWALL_TAG}
fi

for NODE_ID in $(seq 0 `expr $NUM_NODES - 1`)
do (
ZONE=${ZONES[${NODE_ID}]}

gcloud compute instances create ${MACHINE_NAME}-${NODE_ID} \
--image-family=ubuntu-1804-lts  \
--image-project=gce-uefi-images \
--zone=${ZONE} \
--tags=${FIREWALL_TAG} \
--machine-type n1-standard-4 \
--boot-disk-size 200GB \
--zone ${ZONE} \

# Wait for SSH.
until gcloud compute ssh --zone=${ZONE} ${MACHINE_NAME}-${NODE_ID} -- sudo lsblk; do sleep 1; done

# Start image inside.
gcloud compute ssh --zone=${ZONE} ${MACHINE_NAME}-${NODE_ID} -- <<GCLOUD
sudo chmod 777 /opt
cd /opt
mkdir near
sudo apt update
sudo apt install -y pkg-config libssl-dev build-essential cmake
export RUSTUP_HOME=/opt/rustup
export CARGO_HOME=/opt/cargo
curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2019-05-22
source /opt/cargo/env

git clone https://github.com/nearprotocol/nearcore.git
cd nearcore
cargo build -p near

tmux new -s node -d
GCLOUD

# Copy the genesis and key pair into the instance
gcloud compute scp --zone=${ZONE} ${NEAR_HOME}/${PREFIX}${NODE_ID}/* ${MACHINE_NAME}-${NODE_ID}:/opt/near/

) &
done

wait
echo "Started node pool"
gcloud compute instances list --format="value(zone, name, networkInterfaces[0].accessConfigs[0].natIP)" --filter="name~${MACHINE_NAME}"