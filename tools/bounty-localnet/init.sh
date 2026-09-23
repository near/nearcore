#!/bin/bash
# Bounty-localnet init: generates genesis + per-node config, then exits.
# Runs inside the `init` container only — never on the reporter's host.
# Idempotent via a .init-complete marker: same env = no-op, changed env =
# loud failure pointing at ./reset.sh.

set -euo pipefail

: "${NUM_VALIDATORS:?NUM_VALIDATORS is required}"
: "${NUM_SHARDS:?NUM_SHARDS is required}"
: "${CHAIN_ID:?CHAIN_ID is required}"

NETWORK_DIR=/network
SEED_DIR=/tmp/seed
MARKER="$NETWORK_DIR/.init-complete"
REPORTER_KEY=/bounty-localnet/reporter-key.json
REPORTER_ACCOUNT=reporter.test.near
# 1e30 yocto = 1,000,000 NEAR. Kept as a JSON string: jq numbers lose
# precision at this magnitude.
REPORTER_AMOUNT_YOCTO=1000000000000000000000000000000

if (( NUM_VALIDATORS < 1 || NUM_VALIDATORS > 8 )); then
    echo "ERROR: NUM_VALIDATORS must be between 1 and 8 (got: $NUM_VALIDATORS)" >&2
    echo "       The compose file pre-defines validator0..validator7." >&2
    exit 1
fi

# COMPOSE_PROFILES drift from NUM_VALIDATORS crash-loops (too many services)
# or silently stalls at the startup peer gate (too few). Fail loud instead.
# Derived per-run, so deliberately NOT part of the marker hash below.
expected_profiles=""
if (( NUM_VALIDATORS > 1 )); then
    expected_profiles="min$NUM_VALIDATORS"
fi
if [[ "${COMPOSE_PROFILES:-}" != "$expected_profiles" ]]; then
    echo "ERROR: COMPOSE_PROFILES ('${COMPOSE_PROFILES:-}') does not match NUM_VALIDATORS=$NUM_VALIDATORS" >&2
    echo "       Expected: '$expected_profiles'. Fix .env, or use 'just bounty-localnet'" >&2
    echo "       which derives COMPOSE_PROFILES automatically." >&2
    exit 1
fi

mkdir -p "$NETWORK_DIR"

# 1. Marker check — fast exit if already initialized for the same topology.
expected_hash=$(printf '%s|%s|%s' \
    "$NUM_VALIDATORS" "$NUM_SHARDS" "$CHAIN_ID" \
    | sha256sum | cut -d' ' -f1)

if [[ -f "$MARKER" ]]; then
    if [[ "$(cat "$MARKER")" == "$expected_hash" ]]; then
        echo "init: already initialized for current topology, skipping"
        exit 0
    fi
    echo "ERROR: topology changed since init (marker mismatch)." >&2
    echo "       Run ./reset.sh to regenerate, or revert your .env." >&2
    exit 1
fi

# 2. Generate per-node configs from a clean seed dir.
rm -rf "$SEED_DIR"
neard --home "$SEED_DIR" localnet \
    --validators "$NUM_VALIDATORS" \
    --shards "$NUM_SHARDS" \
    --prefix node

# 3. Patch each node's config.json. boot_nodes = all peers except self, via
# docker DNS names. public_addrs emptied so discovery doesn't latch onto the
# random reserved address `neard localnet` writes.
for i in $(seq 0 $((NUM_VALIDATORS - 1))); do
    boot_nodes=""
    for j in $(seq 0 $((NUM_VALIDATORS - 1))); do
        [[ "$j" == "$i" ]] && continue
        pk=$(jq -r '.public_key' "$SEED_DIR/node$j/node_key.json")
        boot_nodes="${boot_nodes}${pk}@validator${j}:24567,"
    done
    boot_nodes="${boot_nodes%,}"

    if [[ "$i" == "0" ]]; then
        rpc_patch='.rpc.addr="0.0.0.0:3030"'
    else
        rpc_patch='.'
    fi

    jq --arg bn "$boot_nodes" \
        ".network.addr = \"0.0.0.0:24567\"
         | .network.public_addrs = []
         | .network.boot_nodes = \$bn
         | $rpc_patch" \
        "$SEED_DIR/node$i/config.json" > "$SEED_DIR/node$i/config.patched.json"
    mv "$SEED_DIR/node$i/config.patched.json" "$SEED_DIR/node$i/config.json"
done

# 4. Patch genesis in place (node0's copy; step 5 fans it out).
REPORTER_PUBKEY=$(jq -r '.public_key' "$REPORTER_KEY")

GENESIS_SRC="$SEED_DIR/node0/genesis.json" \
GENESIS_OUT="$SEED_DIR/genesis.patched.json" \
CHAIN_ID="$CHAIN_ID" \
REPORTER_ACCOUNT="$REPORTER_ACCOUNT" \
REPORTER_PUBKEY="$REPORTER_PUBKEY" \
REPORTER_AMOUNT_YOCTO="$REPORTER_AMOUNT_YOCTO" \
python3 <<'PYEOF'
import json
import os
import sys

src = os.environ["GENESIS_SRC"]
out = os.environ["GENESIS_OUT"]
chain_id = os.environ["CHAIN_ID"]
reporter_account = os.environ["REPORTER_ACCOUNT"]
reporter_pubkey = os.environ["REPORTER_PUBKEY"]
reporter_amount = int(os.environ["REPORTER_AMOUNT_YOCTO"])

with open(src) as f:
    g = json.load(f)

if "validators" not in g or not g["validators"]:
    print("ERROR: genesis has no validators — aborting", file=sys.stderr)
    sys.exit(1)
if "records" not in g:
    print("ERROR: genesis is in records-file mode, not inline records — patcher only handles inline", file=sys.stderr)
    sys.exit(1)

# `neard localnet` writes a random chain id; pin it so reporters can
# hard-code the value in their tooling.
g["chain_id"] = chain_id

# Add reporter account + access key. StateRecord is a serde enum: shape is
# {"Account": {...}}, not flat. code_hash "11..1" = base58 of 32 zero bytes
# (CryptoHash::default()). storage_usage not authoritative — the genesis
# state applier recomputes it (core/store/src/genesis/state_applier.rs).
g["records"].append({
    "Account": {
        "account_id": reporter_account,
        "account": {
            "amount": str(reporter_amount),
            "locked": "0",
            "code_hash": "11111111111111111111111111111111",
            "storage_usage": 182,
        },
    },
})
g["records"].append({
    "AccessKey": {
        "account_id": reporter_account,
        "public_key": reporter_pubkey,
        "access_key": {"nonce": 0, "permission": "FullAccess"},
    },
})

# Keep invariant: sum(amount + locked) == total_supply.
g["total_supply"] = str(int(g["total_supply"]) + reporter_amount)

with open(out, "w") as f:
    json.dump(g, f, indent=2)
PYEOF

mv "$SEED_DIR/genesis.patched.json" "$SEED_DIR/node0/genesis.json"

# 5. Materialize per-node dirs under /network. Same genesis copied to every
# node so all validators share an identical genesis hash.
for i in $(seq 0 $((NUM_VALIDATORS - 1))); do
    node_out="$NETWORK_DIR/node$i"
    rm -rf "$node_out"
    mkdir -p "$node_out"
    cp "$SEED_DIR/node$i/config.json" "$node_out/config.json"
    cp "$SEED_DIR/node$i/validator_key.json" "$node_out/validator_key.json"
    cp "$SEED_DIR/node$i/node_key.json" "$node_out/node_key.json"
    cp "$SEED_DIR/node0/genesis.json" "$node_out/genesis.json"
done

# 6. Marker last — only succeeds if every step above succeeded.
echo "$expected_hash" > "$MARKER"
echo "init: complete (validators=$NUM_VALIDATORS shards=$NUM_SHARDS)"
