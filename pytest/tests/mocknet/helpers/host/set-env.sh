#!/usr/bin/env bash
# Sets variables in neard.env, which the neard unit loads. They take effect
# when neard restarts.
# Usage: set-env.sh NAME=VALUE...
#        set-env.sh --clear
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
lock_host

env_file="$FORKNET_HOME/neard.env"
if [ "$1" = --clear ]; then
    : >"$env_file"
    exit 0
fi
touch "$env_file"
for assignment in "$@"; do
    name=${assignment%%=*}
    # grep exits with 1 when it selects no line.
    grep -v "^$name=" "$env_file" >"$env_file.new" || true
    echo "$assignment" >>"$env_file.new"
    mv "$env_file.new" "$env_file"
done
