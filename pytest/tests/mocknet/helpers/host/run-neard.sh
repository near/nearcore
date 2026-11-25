#!/usr/bin/env bash
# ExecStart of the neard user unit. Runs `neard run` on nodes and
# `neard mirror run` on the traffic generator, with binaries/current.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

# Run the link target, so the process is named neardN and `pgrep neard` finds
# it (get-profile.sh).
neard=$(readlink -f "$BINARIES_DIR/current")

if [ "$IS_TRAFFIC" != 1 ]; then
    exec "$neard" --log-span-events --home "$NEAR_HOME" --unsafe-fast-startup run
fi

# start-neard.sh writes this file when `start-traffic` gets --source-* flags.
TRAFFIC_SOURCE_HOME=
TRAFFIC_SOURCE_CLOUD_ARCHIVE=0
traffic_source="$FORKNET_HOME/traffic-source.env"
if [ -f "$traffic_source" ]; then
    source "$traffic_source"
fi
args=(mirror run --source-home "${TRAFFIC_SOURCE_HOME:-$SOURCE_HOME}" --target-home "$NEAR_HOME")
if [ "$TRAFFIC_SOURCE_CLOUD_ARCHIVE" = 1 ]; then
    args+=(--source-cloud-archive)
fi
secret_file="$NEAR_HOME/setup/mirror-secret.json"
if [ -f "$secret_file" ]; then
    args+=(--secret-file "$secret_file")
else
    args+=(--no-secret)
fi
# start-neard.sh writes this file when `start-traffic` gets --batch-interval-millis.
mirror_config="$FORKNET_HOME/mirror-config.json"
if [ -f "$mirror_config" ]; then
    args+=(--config-path "$mirror_config")
fi
exec "$neard" "${args[@]}"
