#!/usr/bin/env bash
# Starts the neard unit, or restarts it if the binary or the traffic source
# changes.
# Usage: start-neard.sh [--binary-idx N] [--force-restart] [--batch-interval-millis MS]
#     [--source-cloud-archive] [--source-home DIR] [--source-rpc-backup]
# The --source-* flags set where the traffic generator reads the source chain
# from. traffic-source.env keeps them for later starts. A start with any of
# them replaces the saved source, and --source-rpc-backup goes back to
# SOURCE_HOME, the RPC backup home.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

binary_idx=
force_restart=0
batch_interval_millis=
set_source=0
source_cloud_archive=0
source_home=
source_rpc_backup=0
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
    --source-cloud-archive)
        set_source=1
        source_cloud_archive=1
        shift
        ;;
    --source-home)
        set_source=1
        source_home=$2
        shift 2
        ;;
    --source-rpc-backup)
        set_source=1
        source_rpc_backup=1
        shift
        ;;
    *)
        echo "unknown argument: $1" >&2
        exit 1
        ;;
    esac
done

# neard mirror run fails without a source home, and the forknet gets no
# traffic.
if [ "$source_cloud_archive" = 1 ] && [ -z "$source_home" ]; then
    echo "--source-cloud-archive needs --source-home, a cloud archive reader home" >&2
    exit 1
fi
if [ "$source_rpc_backup" = 1 ] && { [ "$source_cloud_archive" = 1 ] || [ -n "$source_home" ]; }; then
    echo "--source-rpc-backup cannot be combined with other --source-* flags" >&2
    exit 1
fi

require_state ready

old_binary=$(readlink "$BINARIES_DIR/current")
if [ -n "$binary_idx" ]; then
    if [ ! -e "$BINARIES_DIR/neard$binary_idx" ]; then
        echo "no neard binary with index $binary_idx" >&2
        exit 1
    fi
    ln -sfn "neard$binary_idx" "$BINARIES_DIR/current"
fi
new_binary=$(readlink "$BINARIES_DIR/current")

source_changed=0
if [ "$set_source" = 1 ] && [ "$IS_TRAFFIC" != 1 ]; then
    echo "warning: ignoring the --source-* flags, this host is not the traffic generator" >&2
fi
if [ "$IS_TRAFFIC" = 1 ]; then
    if [ "$set_source" = 1 ]; then
        source_file="$FORKNET_HOME/traffic-source.env"
        old_source=$(cat "$source_file" 2>/dev/null || true)
        if [ "$source_rpc_backup" = 1 ]; then
            rm -f "$source_file"
        else
            printf 'TRAFFIC_SOURCE_HOME=%q\nTRAFFIC_SOURCE_CLOUD_ARCHIVE=%s\n' \
                "$source_home" "$source_cloud_archive" >"$source_file"
        fi
        if [ "$old_source" != "$(cat "$source_file" 2>/dev/null || true)" ]; then
            source_changed=1
        fi
    fi

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
    if [ "$force_restart" = 1 ] || [ "$old_binary" != "$new_binary" ] ||
        [ "$source_changed" = 1 ]; then
        systemctl --user restart neard
    fi
else
    systemctl --user start neard
fi
