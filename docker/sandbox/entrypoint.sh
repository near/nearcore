#!/bin/sh
set -e

# Suppress logs unless explicitly enabled
# neard::cli=off silences the kernel parameter warnings that are irrelevant for sandbox
if [ "${NEAR_ENABLE_SANDBOX_LOG}" != "1" ]; then
    export RUST_LOG="${RUST_LOG:-neard::cli=off,near=error,stats=error,network=error}"
else
    export RUST_LOG="${RUST_LOG:-neard::cli=off,info}"
fi

# Configurable root account and test seed (deterministic key generation)
NEAR_ROOT_ACCOUNT="${NEAR_ROOT_ACCOUNT:-sandbox}"
NEAR_TEST_SEED="${NEAR_TEST_SEED:-sandbox}"
NEAR_CHAIN_ID="${NEAR_CHAIN_ID:-sandbox}"

# Initialize sandbox data directory if not already done
if [ ! -f /data/genesis.json ]; then
    near-sandbox --home /data init --fast \
        --account-id "$NEAR_ROOT_ACCOUNT" \
        --test-seed "$NEAR_TEST_SEED" \
        --chain-id "$NEAR_CHAIN_ID"
fi

# Apply custom config overrides from /config volume mount
if [ -f /config/genesis.json ]; then
    cp /config/genesis.json /data/genesis.json
fi

if [ -f /config/config.json ]; then
    cp /config/config.json /data/config.json
fi

exec near-sandbox --home /data run --rpc-addr 0.0.0.0:3030 --network-addr 0.0.0.0:3031
