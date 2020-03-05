#!/bin/bash
set -euo pipefail

INSTANCES=("34.94.160.151" "104.196.39.111" "146.148.94.78" "35.228.209.219")
IMAGE=nearprotocol/nearcore:master

function stop_node {
    ssh -o StrictHostKeyChecking=no bo_nearprotocol_com@$1 $SSH_KEY -- <<SSH
docker stop nearcore || true
docker rm nearcore || true
SSH
}

function clear_data {
    ssh -o StrictHostKeyChecking=no bo_nearprotocol_com@$1 $SSH_KEY -- <<SSH
rm -rf /home/bo_nearprotocol_com/.near
SSH
}

function upload_dir {
    scp -r -o StrictHostKeyChecking=no $SSH_KEY /tmp/near/node$2 bo_nearprotocol_com@$1:/home/bo_nearprotocol_com/.near
}

function start_node {
    ssh -o StrictHostKeyChecking=no bo_nearprotocol_com@$1 $SSH_KEY -- <<SSH
docker pull nearprotocol/nearcore:master
docker run -d -u \$UID:\$UID -v /home/bo_nearprotocol_com/.near:/srv/near --restart unless-stopped \
    -p 3030:3030 -p 24567:24567 --name nearcore nearprotocol/nearcore:master near --home=/srv/near run --boot-nodes=$boot_nodes
docker logs nearcore
SSH
}

function get_bootnode {
    echo $(jq -r '.public_key' $1/node_key.json)@$2:24567
}

# Update image
docker pull nearprotocol/nearcore:master
# Init four configs
rm -rf /tmp/near/node*
mkdir -p /tmp/near
docker run -u $UID:$UID -v /tmp/near:/srv/near nearprotocol/nearcore:master near --home=/srv/near testnet --shards 1
# Replace boot nodes in configs
boot_nodes=$(get_bootnode /tmp/near/node0 ${INSTANCES[0]})
for i in $(seq 1 $(( "${#INSTANCES[@]}" - 1 ))); do
    boot_nodes="${boot_nodes},$(get_bootnode /tmp/near/node${i} ${INSTANCES[i]})"
done
for i in "${!INSTANCES[@]}"; do
    jq --arg b $boot_nodes '.network.boot_nodes=$b' /tmp/near/node${i}/config.json > /tmp/near/node${i}/config.json.tmp
    mv /tmp/near/node${i}/config.json.tmp /tmp/near/node${i}/config.json
done
# Deploy
for i in "${!INSTANCES[@]}"; do
    e="${INSTANCES[$i]}"

    stop_node $e
    clear_data $e
    upload_dir $e $i
    start_node $e
done
