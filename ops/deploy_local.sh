#!/bin/bash
set -e

IMAGE=${1:-nearprotocol/nearcore:0.1.1}
STUDIO_IMAGE=${2:-nearprotocol/studio:0.2.4}
TOTAL_NODES=2

sudo docker run -d --name testnet-0 -p 3030:3030 -p 26656:26656 --rm \
	-e "BOOT_NODES=127.0.0.1:26658" \
	-e "NODE_ID=0" \
	-e "TOTAL_NODES=${TOTAL_NODES}" \
	-e "NODE_KEY=53Mr7IhcJXu3019FX+Ra+VbxSQ5y2q+pknmM463jzoFzldWZb16dSYRxrhYrLRXe/UA0wR2zFy4c3fY5yDHjlA==" \
	${IMAGE}

sudo docker run -d --name testnet-1 -p 3031:3030 -p 26670:26656 --add-host=testnet-0:172.17.0.2 --rm \
	-e "BOOT_NODES=6f99d7b49a10fff319cd8bbbd13c3a964dcd0248@172.17.0.2:26656" \
	-e "NODE_ID=1" \
	-e "TOTAL_NODES=${TOTAL_NODES}" \
	${IMAGE}

sudo docker run -d --name studio -p 80:80 --add-host=testnet-0:172.17.0.2 --rm \
    -e "DEVNET_HOST=http://172.17.0.2" \
    -e "NEARLIB_COMMIT=348509b526cf4ca0495d86cb211d1013d84629a2" \
    -e "NEARLIB_VERSION=0.5.2" \
    -e "EXTERNAL_HOST_NAME=http://localhost" \
    ${STUDIO_IMAGE}

STUDIO_IP=localhost

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
        STATUS_CODE=$(curl -I ${STUDIO_IP} 2>/dev/null | head -n 1 | cut -d$' ' -f2);
        if [[ ${STATUS_CODE} -eq 200 ]]; then
            exit 0
        fi
        sleep 1
    done
}

wait_for_studio & spinner
echo "NEARStudio is now accessible at http://${STUDIO_IP}"
