#!/usr/bin/env bash
# Second step of `mirror.py new-test`, after mirror.py uploads
# validators.json.new and setup.env.new. Moves them into place, patches
# config.json and starts setup-node.sh in the background.
# Usage: network-init.sh CONFIG_CHANGES
# CONFIG_CHANGES is passed to patch-config.sh.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
lock_host

require_state awaiting-network-init
mv "$FORKNET_HOME/validators.json.new" "$FORKNET_HOME/validators.json"
mv "$FORKNET_HOME/setup.env.new" "$FORKNET_HOME/setup.env"
bash "$FORKNET_HOME/patch-config.sh" "$1"
# Write the state before the unit starts, so that a second network-init fails.
echo setting-up >"$STATE_FILE"
if ! run_in_unit neard-setup setup-node.sh; then
    echo "error: failed to start neard-setup" >"$STATE_FILE"
    exit 1
fi
