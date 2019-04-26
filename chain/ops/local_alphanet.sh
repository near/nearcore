#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/alphanet:0.1.6}
STUDIO_IMAGE=${2:-nearprotocol/studio:0.1.8}

sudo docker run -d --name alphanet-0 -p 3000:3000 -p 3030:3030 \
    -e "BOOT_NODE_IP=127.0.0.1" \
    -e "NODE_NUM=0" \
    -e "TOTAL_NODES=4" \
    ${IMAGE}

sudo docker run -d --name alphanet-1 --add-host=alphanet-0:172.17.0.2 -p 3031:3030 \
    -e "BOOT_NODE_IP=172.17.0.2" \
    -e "NODE_NUM=1" \
    -e "TOTAL_NODES=4" \
    ${IMAGE}

sudo docker run -d --name alphanet-2 --add-host=alphanet-0:172.17.0.2 -p 3032:3030 \
    -e "BOOT_NODE_IP=172.17.0.2" \
    -e "NODE_NUM=2" \
    -e "TOTAL_NODES=4" \
    ${IMAGE}

sudo docker run -d --name alphanet-3 --add-host=alphanet-0:172.17.0.2 -p 3033:3030 \
    -e "BOOT_NODE_IP=172.17.0.2" \
    -e "NODE_NUM=3" \
    -e "TOTAL_NODES=4" \
    ${IMAGE}

sudo docker run -d --name studio -p 80:80 --add-host=alphanet-0:172.17.0.2 \
    -e "DEVNET_HOST=http://172.17.0.2" \
    ${STUDIO_IMAGE}

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

wait_for_studio()
{
    while :
    do
        STATUS_CODE=$(curl -I localhost 2>/dev/null | head -n 1 | cut -d$' ' -f2);
        if [[ ${STATUS_CODE} -eq 200 ]]; then
            exit 0
        fi
        sleep 1
    done
}

echo "Alphanet HTTP interface is accessible at 127.0.0.1:3030"
echo "Waiting for studio instance to start. This could take a few minutes..."
wait_for_studio & spinner
echo "NEARStudio is now accessible at http://localhost"
