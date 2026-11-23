#!/usr/bin/env bash
# Prints the state of the test. See STATE_FILE in common.sh.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

cat "$STATE_FILE" 2>/dev/null || echo "no test"
