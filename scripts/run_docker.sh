#!/bin/bash
set -e

export NEAR_HOME=/srv/near

ulimit -c unlimited

echo "Telemetry: ${TELEMETRY_URL}"
echo "Bootnodes: ${BOOT_NODES}"

if [[ -z "${VERBOSE}" ]]; then
    verbose="--verbose ''"
fi

near --home=${NEAR_HOME} run --telemetry-url=${TELEMETRY_URL} --boot-nodes=${BOOT_NODES} ${verbose}
