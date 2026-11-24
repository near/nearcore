#!/usr/bin/env bash
# Replaces the URL of one neard binary and downloads the binaries again.
# Usage: amend-binary.sh INDEX URL
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"

index=$1
url=$2
urls="$BINARIES_DIR/urls"
if [ "$index" -lt 0 ] || [ "$(wc -l <"$urls")" -le "$index" ]; then
    echo "no neard binary with index $index" >&2
    exit 1
fi
awk -v n=$((index + 1)) -v url="$url" 'NR == n { $0 = url } { print }' "$urls" >"$urls.new"
mv "$urls.new" "$urls"
bash "$FORKNET_HOME/download-binaries.sh"
