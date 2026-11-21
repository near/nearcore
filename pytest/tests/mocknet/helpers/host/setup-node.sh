#!/usr/bin/env bash
# Second step of `mirror.py new-test`. mirror.py writes setup.env and
# validators.json, patches config.json, and starts this script as the
# neard-setup user unit. The script builds the genesis state of the test
# chain and saves it as the start backup. mirror.py polls the state file until
# it reads "ready".
#
# setup.env sets:
#   MOCKNET_ID        id of the test network
#   CHUNK_VALIDATOR   1 if the node starts without state (role "validator")
#   UPLOAD_SETUP      1 if the node uploads genesis for the chunk validators
#   STATE_SOURCE, PATCHES_PATH, EPOCH_LENGTH, NUM_SEATS, GENESIS_TIME,
#   NEW_CHAIN_ID, PROTOCOL_VERSION
#                     arguments of `neard fork-network set-validators`
#   CLI_CHAIN_ID, RPC_ADDR
#                     arguments of setup-near-cli.sh
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
trap 'rc=$?; if [ $rc -ne 0 ]; then echo "error: setup-node.sh exited with code $rc, see $FORKNET_HOME/setup.log" >"$STATE_FILE"; fi' EXIT
source "$FORKNET_HOME/setup.env"

exec >>"$FORKNET_HOME/setup.log" 2>&1
echo setting-up >"$STATE_FILE"
echo "$(date -u) setup started"

if ! bash "$FORKNET_HOME/setup-near-cli.sh" "$CLI_CHAIN_ID" "$RPC_ADDR"; then
    echo "near-cli setup failed"
fi

remote_setup_path="$MOCKNET_STORE_PATH/$MOCKNET_ID/$RUN_ID"

if [ "$CHUNK_VALIDATOR" = 1 ]; then
    # Chunk validators start without state. They download genesis and the
    # epoch configs after the first validator uploads them.
    while true; do
        rc=0
        gsutil ls "$remote_setup_path" >/dev/null || rc=$?
        if [ $rc -eq 0 ]; then
            break
        fi
        # gsutil ls exits with 1 when the path does not exist.
        if [ $rc -ne 1 ]; then
            exit $rc
        fi
        sleep 10
    done
    gsutil -m rsync -r "$remote_setup_path" "$NEAR_HOME"
    echo ready >"$STATE_FILE"
    echo "$(date -u) setup finished"
    exit 0
fi

if [ "$STATE_SOURCE" = empty ]; then
    rm -rf "$NEAR_HOME/data"
fi

args=(
    --home "$NEAR_HOME" fork-network set-validators
    --validators "$FORKNET_HOME/validators.json"
    --epoch-length "$EPOCH_LENGTH"
    --genesis-time "$GENESIS_TIME"
    --num-seats "$NUM_SEATS"
    --state-source "$STATE_SOURCE"
)
if [ -n "$PATCHES_PATH" ]; then
    args+=(--patches-path "$PATCHES_PATH")
fi
if [ -n "$NEW_CHAIN_ID" ]; then
    args+=(--chain-id "$NEW_CHAIN_ID")
fi
if [ -n "$PROTOCOL_VERSION" ]; then
    args+=(--protocol-version "$PROTOCOL_VERSION")
fi
"$NEARD0" "${args[@]}"
"$NEARD0" --home "$NEAR_HOME" fork-network finalize

if [ "$UPLOAD_SETUP" = 1 ]; then
    upload_dir=$(mktemp -d)
    cp "$NEAR_HOME/genesis.json" "$upload_dir/"
    cp -r "$NEAR_HOME/epoch_configs" "$upload_dir/"
    gsutil -m rsync -r "$upload_dir" "$remote_setup_path"
    rm -rf "$upload_dir"
fi

# make-snapshot hard links the immutable sst files, so the backup is cheap.
mkdir -p "$(dirname "$START_BACKUP")"
"$NEARD0" --home "$NEAR_HOME" --unsafe-fast-startup database make-snapshot --destination "$START_BACKUP"
cp "$NEAR_HOME/config.json" "$NEAR_HOME/genesis.json" "$NEAR_HOME/node_key.json" "$START_BACKUP/"
# Keep the binary that made the backup. A later binary may not be able to
# open it.
cp "$NEARD0" "$START_BACKUP/neard"

echo ready >"$STATE_FILE"
echo "$(date -u) setup finished"
