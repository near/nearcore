#!/usr/bin/env bash
# ExecStart of the neard user unit. Runs `neard run` on nodes and
# `neard mirror run` on the traffic generator, with binaries/current.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

neard="$BINARIES_DIR/current"

if [ "$IS_TRAFFIC" != 1 ]; then
    exec "$neard" --log-span-events --home "$NEAR_HOME" --unsafe-fast-startup run
fi

args=(mirror run --source-home "$SOURCE_HOME" --target-home "$NEAR_HOME")
secret_file="$NEAR_HOME/setup/mirror-secret.json"
if [ -f "$secret_file" ]; then
    args+=(--secret-file "$secret_file")
else
    args+=(--no-secret)
fi
# mirror.py writes this file when `start-traffic` gets --batch-interval-millis.
mirror_config="$FORKNET_HOME/mirror-config.json"
if [ -f "$mirror_config" ]; then
    args+=(--config-path "$mirror_config")
fi
exec "$neard" "${args[@]}"
