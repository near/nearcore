#!/bin/bash

if [[ -z "${1}" ]]; then
    echo "Usage: generate_dashboard_for_network.sh network_name_in_prometheus"
    exit 1
fi

sed "s/staging-testnet/${1}/g" $(dirname $0)/grafana-dashboard.json >  $(dirname $0)/grafana-dashboard-${1}.json