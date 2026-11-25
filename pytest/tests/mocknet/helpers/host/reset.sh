#!/usr/bin/env bash
# Starts reset-node.sh in the background. Writes the "resetting" state first,
# so that a poll right after this script does not see "ready".
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
lock_host

require_state ready
echo resetting >"$STATE_FILE"
if ! run_in_unit neard-reset reset-node.sh; then
    echo "error: failed to start neard-reset" >"$STATE_FILE"
    exit 1
fi
