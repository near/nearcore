#!/bin/sh
set -e

NEAR_HOME=${NEAR_HOME:-/srv/near}
export NEAR_HOME

if [ -n "$INIT" ]; then
    neard init ${CHAIN_ID:+--chain-id="$CHAIN_ID"} \
               ${ACCOUNT_ID:+--account-id="$ACCOUNT_ID"}
fi

if [ -n "$NODE_KEY" ]; then
    cat << EOF > "$NEAR_HOME/node_key.json"
{"account_id": "", "public_key": "", "secret_key": "$NODE_KEY"}
EOF
fi

ulimit -c unlimited

echo "Telemetry: ${TELEMETRY_URL}"
echo "Bootnodes: ${BOOT_NODES}"

exec neard run ${TELEMETRY_URL:+--telemetry-url="$TELEMETRY_URL"} \
               ${BOOT_NODES:+--boot-nodes="$BOOT_NODES"} "$@"
