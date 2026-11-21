#!/usr/bin/env bash
# Starts the neard unit, or restarts it if the binary changes.
# Usage: start-neard.sh [--binary-idx N] [--force-restart] [--batch-interval-millis MS]
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

binary_idx=
force_restart=0
batch_interval_millis=
while [ $# -gt 0 ]; do
    case "$1" in
    --binary-idx)
        binary_idx=$2
        shift 2
        ;;
    --force-restart)
        force_restart=1
        shift
        ;;
    --batch-interval-millis)
        batch_interval_millis=$2
        shift 2
        ;;
    *)
        echo "unknown argument: $1" >&2
        exit 1
        ;;
    esac
done

state=$(cat "$STATE_FILE" 2>/dev/null || echo "no test")
if [ "$state" != ready ]; then
    echo "cannot start neard: state is $state" >&2
    exit 1
fi

old_binary=$(readlink "$BINARIES_DIR/current")
if [ -n "$binary_idx" ]; then
    if [ ! -e "$BINARIES_DIR/neard$binary_idx" ]; then
        echo "no neard binary with index $binary_idx" >&2
        exit 1
    fi
    ln -sfn "neard$binary_idx" "$BINARIES_DIR/current"
fi
new_binary=$(readlink "$BINARIES_DIR/current")

if [ "$IS_TRAFFIC" = 1 ]; then
    mirror_config="$FORKNET_HOME/mirror-config.json"
    if [ -n "$batch_interval_millis" ]; then
        jq -n --argjson ms "$batch_interval_millis" \
            '{tx_batch_interval: {secs: ($ms / 1000 | floor), nanos: ($ms % 1000 * 1000000)}}' \
            >"$mirror_config"
    else
        rm -f "$mirror_config"
    fi
fi

# systemd counts manual starts and restarts against the unit's start limit,
# which is meant only for crash restarts. Reset the count before each manual
# start. reset-failed fails when the unit is not loaded, and then there is no
# count to reset.
systemctl --user reset-failed neard 2>/dev/null || true
if systemctl --user is-active --quiet neard; then
    if [ "$force_restart" = 1 ] || [ "$old_binary" != "$new_binary" ]; then
        systemctl --user restart neard
    fi
else
    systemctl --user start neard
fi
