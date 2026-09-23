#!/bin/bash
# Tear down the stack and wipe generated state. Required after any .env
# topology change.

set -euo pipefail

cd "$(dirname "$0")"

# A failed `down` (e.g. broken .env) shouldn't abort the wipe — warn only.
docker compose down -v --remove-orphans \
    || echo "warning: docker compose down failed; containers may still be running" >&2

# `down -v` doesn't touch bind mounts. On Linux ./network may hold root-owned
# files; fall back to wiping from inside a container. -mindepth 1 because the
# bind mountpoint itself can't be removed from inside the mount.
if [[ -d ./network ]] && ! rm -rf ./network/ 2>/dev/null; then
    docker run --rm -v "$PWD/network:/n" ubuntu:22.04 find /n -mindepth 1 -delete
    rmdir ./network
fi

echo "State cleared. Run 'docker compose up --build' to start fresh."
