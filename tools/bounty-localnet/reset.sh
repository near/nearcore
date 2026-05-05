#!/bin/bash
# Tear down the bounty-localnet stack and wipe local state. Use this when you
# change topology in .env (NUM_VALIDATORS, NUM_SHARDS, etc.) so that the next
# `docker compose up` regenerates configs cleanly.

set -euo pipefail

cd "$(dirname "$0")"

# `down -v` removes named/anonymous volumes, but bind mounts (./network) are
# untouched — we have to rm them ourselves.
docker compose down -v --remove-orphans
rm -rf ./network/

echo "State cleared. Run 'docker compose up --build' to start fresh."
