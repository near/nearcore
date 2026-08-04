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

# The sandbox produces a block every 120ms, which needs a higher gc_blocks_limit than the
# default of 2 - config validation rejects a gc that cannot keep up with block production.
# `init` writes a suitable value, but a /data volume or a /config override created before that
# was enforced does not. Repair only a config that is already rejected, so a healthy one is
# never rewritten. 9 is ceil(2 * 500ms gc_step_period / 120ms block time).
# `--unsafe-fast-startup` skips the genesis consistency checks that `run` repeats below; only
# config validation is relevant here, and it runs regardless of that flag.
if [ -f /data/config.json ] &&
    ! near-sandbox --home /data --unsafe-fast-startup validate-config >/dev/null 2>&1; then
    # Same filesystem as the target, so the replacement is an atomic rename.
    repaired=$(mktemp /data/config.json.XXXXXX)
    if jq '.gc_blocks_limit = 9' /data/config.json >"$repaired" 2>/dev/null; then
        mv "$repaired" /data/config.json
    else
        rm -f "$repaired"
    fi
fi

exec near-sandbox --home /data run --rpc-addr 0.0.0.0:3030 --network-addr 0.0.0.0:3031
