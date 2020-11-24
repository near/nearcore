#!/bin/bash
set -e

export NEAR_HOME=/srv/near

if [[ -z {INIT} ]]
then
    neard --home=${NEAR_HOME} init --chain-id=${CHAIN_ID} --account-id=${ACCOUNT_ID}
fi

if [[ -z {NODE_KEY} ]]
then

    cat << EOF > ${NEAR_HOME}/node_key.json
{"account_id": "", "public_key": "", "secret_key": "$NODE_KEY"}
EOF

fi

ulimit -c unlimited

echo "Telemetry: ${TELEMETRY_URL}"
echo "Bootnodes: ${BOOT_NODES}"

neard --home=${NEAR_HOME} run --telemetry-url=${TELEMETRY_URL} --boot-nodes=${BOOT_NODES}
