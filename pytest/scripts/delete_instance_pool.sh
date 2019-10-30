#!/bin/bash
MACHINE_NAME=${1:-near-pytest}
INSTANCES=$(gcloud compute instances list  --filter="tags.items=${MACHINE_NAME}" --format="value[separator=' --zone '](name, zone)") \

while read -r line; do
    $(yes | gcloud compute instances delete --delete-disks all $line) &
done <<< "$INSTANCES"

yes | gcloud compute firewall-rules delete "${MACHINE_NAME}"
wait
