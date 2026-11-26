#!/usr/bin/env bash
# Downloads the neard binaries listed in binaries/urls, one URL per line, to
# binaries/neard0, binaries/neard1, and so on. A URL that is a path to a local
# file is linked instead.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
lock_host

mkdir -p "$BINARIES_DIR"
i=0
while read -r url; do
    dst="$BINARIES_DIR/neard$i"
    if [ -f "$url" ]; then
        # Uploaded files, such as CI artifacts, may not be executable.
        chmod 755 "$url"
        ln -sfn "$url" "$dst"
    else
        # Download to a temporary file so that a failed download does not
        # replace a good binary.
        curl -fsSL --retry 3 --proto '=https,http' -o "$dst.tmp" -- "$url"
        chmod 755 "$dst.tmp"
        mv "$dst.tmp" "$dst"
    fi
    echo "neard$i: $url"
    i=$((i + 1))
done <"$BINARIES_DIR/urls"

if [ ! -e "$BINARIES_DIR/current" ]; then
    ln -sfn neard0 "$BINARIES_DIR/current"
fi
