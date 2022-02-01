#!/bin/sh
set -e

export NEAR_HOME=/srv/near

NEARD_FLAGS=${NEAR_HOME:+--home="$NEAR_HOME"}

if [ -n "$INIT" ]
then
    neard "$NEARD_FLAGS" init ${CHAIN_ID:+--chain-id="$CHAIN_ID"} ${ACCOUNT_ID:+--account-id="$ACCOUNT_ID"}
fi

if [ -n "$NODE_KEY" ]
then
    cat << EOF > "$NEAR_HOME/node_key.json"
{"account_id": "", "public_key": "", "secret_key": "$NODE_KEY"}
EOF
fi

ulimit -c unlimited

echo "Telemetry: ${TELEMETRY_URL}"
echo "Bootnodes: ${BOOT_NODES}"

exec neard "$NEARD_FLAGS" run ${TELEMETRY_URL:+--telemetry-url="$TELEMETRY_URL"} ${BOOT_NODES:+--boot-nodes="$BOOT_NODES"} "$@"
