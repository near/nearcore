#!/usr/bin/env bash
# Installs the user units from systemd/ and downloads the neard binaries.
# mirror.py runs it from `init` after it uploads this directory, host.env and
# binaries/urls.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

UNIT_DIR="$HOME/.config/systemd/user"
mkdir -p "$UNIT_DIR" "$HOME/neard-logs"

# The units use the fixed path ~/.near/forknet. On the traffic generator the
# forknet home is on the target disk, so link it there.
if [ "$FORKNET_HOME" != "$HOME/.near/forknet" ]; then
    ln -sfn "$FORKNET_HOME" "$HOME/.near/forknet"
fi

cp "$FORKNET_HOME"/systemd/neard.service "$FORKNET_HOME"/systemd/neard-logrotate.* "$UNIT_DIR/"
if [ "$IS_TRAFFIC" = 1 ]; then
    mkdir -p "$UNIT_DIR/neard.service.d"
    cp "$FORKNET_HOME/systemd/neard-traffic.conf" "$UNIT_DIR/neard.service.d/"
fi

systemctl --user daemon-reload
systemctl --user enable --now neard-logrotate.timer

bash "$FORKNET_HOME/download-binaries.sh"
