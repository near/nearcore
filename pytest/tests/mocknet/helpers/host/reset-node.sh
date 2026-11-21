#!/usr/bin/env bash
# Resets the test chain to the start backup that setup-node.sh made.
# mirror.py sets the state to "resetting", starts this script as the
# neard-reset user unit, and polls the state file until it reads "ready".
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
trap 'rc=$?; if [ $rc -ne 0 ]; then echo "error: reset-node.sh exited with code $rc, see $FORKNET_HOME/setup.log" >"$STATE_FILE"; fi' EXIT
source "$FORKNET_HOME/setup.env"

exec >>"$FORKNET_HOME/setup.log" 2>&1
echo "$(date -u) reset started"

systemctl --user stop neard
ln -sfn neard0 "$BINARIES_DIR/current"
rm -rf "$NEAR_HOME/data"

# Chunk validators start without state, so an empty data dir is their start
# state.
if [ "$CHUNK_VALIDATOR" != 1 ]; then
    "$START_BACKUP/neard" --home "$START_BACKUP" --unsafe-fast-startup \
        database make-snapshot --destination "$NEAR_HOME"
fi

remote_setup_path="$MOCKNET_STORE_PATH/$MOCKNET_ID/$RUN_ID"
if [ "$UPLOAD_SETUP" = 1 ] && gsutil ls "$remote_setup_path" >/dev/null 2>&1; then
    gsutil -m rm -r "$remote_setup_path"
fi

echo ready >"$STATE_FILE"
echo "$(date -u) reset finished"
