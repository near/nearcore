#!/bin/bash
set -e

STUDIO_IMAGE=${3:-nearprotocol/studio:0.1.8}
PREFIX=${2:-alphanet}
ZONE=${3:-us-west2-a}

gcloud beta compute instances update-container ${PREFIX}-studio \
    --zone ${ZONE} \
    --container-image ${STUDIO_IMAGE}

# borrowed from https://stackoverflow.com/a/20369590
spinner()
{
    local pid=$!
    local delay=0.75
    local spinstr='|/-\'
    while [[ "$(ps a | awk '{print $1}' | grep $pid)" ]]; do
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
    --zone us-west2-a | grep natIP | \
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

echo "Waiting for studio instance to start. This could take a few minutes..."
wait_for_studio & spinner
echo "NEARStudio is now accessible at http://${STUDIO_IP}"
