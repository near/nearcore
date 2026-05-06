#!/bin/bash
# Bounty-localnet init: generates genesis + per-node config for an N-validator
# localnet, then exits. Intended to run inside the `init` container of
# docker-compose.yml — never on the reporter's host.
#
# The init step is idempotent: a completion marker (.init-complete) records
# the env vars used; a re-run with the same values is a no-op, and a re-run
# with different values fails loudly and tells the reporter to ./reset.sh.

set -euo pipefail

: "${NUM_VALIDATORS:?NUM_VALIDATORS is required}"
: "${NUM_SHARDS:?NUM_SHARDS is required}"
: "${STAKE_DISTRIBUTION:?STAKE_DISTRIBUTION is required}"
: "${CHAIN_ID:?CHAIN_ID is required}"

NETWORK_DIR=/network
SEED_DIR=/tmp/seed
MARKER="$NETWORK_DIR/.init-complete"
REPORTER_KEY=/bounty-localnet/reporter-key.json
REPORTER_ACCOUNT=reporter.test.near
# 1e30 yocto = 1,000,000 NEAR — emitted as a JSON string everywhere, since
# jq's number type loses precision at this magnitude (the same trap that
# benchmarks/transactions-generator/justfile documents for gas_limit).
REPORTER_AMOUNT_YOCTO=1000000000000000000000000000000

case "$STAKE_DISTRIBUTION" in
    uniform|skewed) ;;
    *)
        echo "ERROR: STAKE_DISTRIBUTION must be 'uniform' or 'skewed', got: $STAKE_DISTRIBUTION" >&2
        exit 1
        ;;
esac

if (( NUM_VALIDATORS < 1 || NUM_VALIDATORS > 8 )); then
    echo "ERROR: NUM_VALIDATORS must be between 1 and 8 (got: $NUM_VALIDATORS)" >&2
    echo "       The compose file pre-defines validator0..validator7." >&2
    exit 1
fi

mkdir -p "$NETWORK_DIR"

# 1. Marker check — fast exit if already initialized for the same topology.
expected_hash=$(printf '%s|%s|%s|%s' \
    "$NUM_VALIDATORS" "$NUM_SHARDS" "$STAKE_DISTRIBUTION" "$CHAIN_ID" \
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

# 3. Patch each node's config.json. boot_nodes excludes self; addresses use
# docker DNS service names (validator0..validatorN-1). public_addrs is
# emptied so peer discovery doesn't latch onto the random reserved address
# `neard localnet` writes by default.
#
# We also lower `consensus.min_num_peers` so the chain keeps producing
# blocks when one validator is byzantine or offline — essential for the
# bounty workflow where a reporter's override binary may be a stub or a
# misbehaving variant. `neard localnet`'s default is N-1 (every node
# expects every other), which stalls the chain on a single failure.
# N-2 lets one validator drop without freezing consensus; floor at 0 for
# N=1 / N=2 where no value of min_num_peers can rescue the network anyway.
min_peers=$(( NUM_VALIDATORS > 2 ? NUM_VALIDATORS - 2 : 0 ))

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

    jq --arg bn "$boot_nodes" --argjson mp "$min_peers" \
        ".network.addr = \"0.0.0.0:24567\"
         | .network.public_addrs = []
         | .network.boot_nodes = \$bn
         | .consensus.min_num_peers = \$mp
         | $rpc_patch" \
        "$SEED_DIR/node$i/config.json" > "$SEED_DIR/node$i/config.patched.json"
    mv "$SEED_DIR/node$i/config.patched.json" "$SEED_DIR/node$i/config.json"
done

# 4. Patch genesis (once; we then copy the patched file to every node).
# This mutates the file in /tmp/seed/node0/genesis.json so that step 5 picks
# the patched version up.
REPORTER_PUBKEY=$(jq -r '.public_key' "$REPORTER_KEY")

GENESIS_SRC="$SEED_DIR/node0/genesis.json" \
GENESIS_OUT="$SEED_DIR/genesis.patched.json" \
CHAIN_ID="$CHAIN_ID" \
STAKE_DISTRIBUTION="$STAKE_DISTRIBUTION" \
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
distribution = os.environ["STAKE_DISTRIBUTION"]
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

# `neard localnet` writes a random chain id (test-chain-XXXXX); honor the
# CHAIN_ID knob from .env so reporters can pin their tooling to a known
# value.
g["chain_id"] = chain_id

# --- Staking preset ---
# `uniform` is a no-op (neard localnet emits equal stakes already).
# `skewed` raises the lexicographically smallest validator's stake to 7x.
# We keep `account.amount + account.locked` constant for that account, so
# total_supply doesn't drift before we bump it for the reporter below.
if distribution == "skewed":
    small = sorted(v["account_id"] for v in g["validators"])[0]
    base_stake = int(next(v["amount"] for v in g["validators"] if v["account_id"] == small))
    new_stake = base_stake * 7
    delta = new_stake - base_stake

    found_validator = False
    for v in g["validators"]:
        if v["account_id"] == small:
            v["amount"] = str(new_stake)
            found_validator = True
    if not found_validator:
        print(f"ERROR: validator {small} not found when applying skewed preset", file=sys.stderr)
        sys.exit(1)

    found_account = False
    for r in g["records"]:
        acct = r.get("Account")
        if acct is None or acct.get("account_id") != small:
            continue
        inner = acct["account"]
        existing_amount = int(inner["amount"])
        if existing_amount < delta:
            print(
                f"ERROR: validator {small} account.amount ({existing_amount}) < delta ({delta}); "
                f"cannot apply skewed preset without breaking total_supply",
                file=sys.stderr,
            )
            sys.exit(1)
        inner["locked"] = str(new_stake)
        inner["amount"] = str(existing_amount - delta)
        found_account = True
    if not found_account:
        print(f"ERROR: Account record for {small} not found", file=sys.stderr)
        sys.exit(1)

# --- Add reporter account + access key ---
# StateRecord is a serde enum, so the JSON shape is {"Account": {...}} /
# {"AccessKey": {...}}, NOT a flat object.
# code_hash "11..1" is base58 of 32 zero bytes (CryptoHash::default()).
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

# Bump total_supply to keep the genesis-validator invariant
# sum(account.amount + account.locked) == total_supply.
g["total_supply"] = str(int(g["total_supply"]) + reporter_amount)

with open(out, "w") as f:
    json.dump(g, f, indent=2)
PYEOF

mv "$SEED_DIR/genesis.patched.json" "$SEED_DIR/node0/genesis.json"

# 5. Materialize per-node directories under /network and copy the patched
# genesis everywhere. We deliberately copy the same genesis to every node so
# that all validators share identical genesis hash.
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
echo "init: complete (validators=$NUM_VALIDATORS shards=$NUM_SHARDS distribution=$STAKE_DISTRIBUTION)"
