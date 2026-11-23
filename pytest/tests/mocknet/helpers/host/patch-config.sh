#!/usr/bin/env bash
# Sets values in the neard config.json.
# Usage: patch-config.sh CHANGES
# CHANGES is a JSON list of [path, value] pairs, for example
# '[[["consensus", "min_num_peers"], 1]]'. Missing objects on a path are
# created.
# cspell:words setpath
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

config="$NEAR_HOME/config.json"
jq --argjson changes "$1" 'reduce $changes[] as [$path, $value] (.; setpath($path; $value))' \
    "$config" >"$config.new"
mv "$config.new" "$config"
