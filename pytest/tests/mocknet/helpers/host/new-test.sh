#!/usr/bin/env bash
# First step of `mirror.py new-test`. Stops neard, creates new keys and
# config, and resets the data dir from the setup dir that the forknet image
# provides. Prints the keys as JSON on the last line of stdout. mirror.py uses
# them to build the validator list and the boot nodes for setup-node.sh.
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
lock_host

# The setup and reset units are transient and exist only while they run.
for unit in neard-setup neard-reset; do
    systemctl --user stop "$unit" 2>/dev/null || true
done
systemctl --user stop neard
echo new-test >"$STATE_FILE"

tmp_home="$FORKNET_HOME/tmp-near-home"
rm -rf "$tmp_home" "$FORKNET_HOME/validators.json" "$FORKNET_HOME/setup.env" \
    "$FORKNET_HOME/backups"
ln -sfn neard0 "$BINARIES_DIR/current"

init_args=(--home "$tmp_home" init)
if [ "$IS_TRAFFIC" != 1 ]; then
    # Assumes the last part of the host name is unique.
    init_args+=(--account-id "node-$(hostname | awk -F- '{print $NF}' | cut -c1-6)")
fi
run_neard0 "${init_args[@]}" >&2

jq '.rpc.addr = "0.0.0.0:3030"
    | .network.addr = "0.0.0.0:24567"
    | .network.allow_private_ip_in_public_addrs = true
    | .network.skip_sync_wait = false
    | .tracked_shards_config = "AllShards"
    | .log_summary_style = "plain"
    | .rpc.enable_debug_rpc = true
    | .consensus.min_block_production_delay = {secs: 0, nanos: 600000000}
    | .consensus.max_block_production_delay = {secs: 1, nanos: 800000000}
    | .consensus.chunk_wait_mult = [1, 3]
    | .consensus.doomslug_step_period = {secs: 0, nanos: 10000000}' \
    "$tmp_home/config.json" >"$tmp_home/config.json.new"
mv "$tmp_home/config.json.new" "$tmp_home/config.json"

mkdir -p "$NEAR_HOME"
find "$NEAR_HOME" -maxdepth 1 -type f -delete
rm -rf "$NEAR_HOME/data"
setup_dir="$NEAR_HOME/setup"
if [ -d "$setup_dir" ]; then
    run_neard0 --home "$setup_dir" database run-migrations >&2
    run_neard0 --home "$setup_dir" database make-snapshot --destination "$NEAR_HOME" >&2
    cp "$setup_dir/genesis.json" "$NEAR_HOME/genesis.json"
else
    echo "no setup dir at $setup_dir, the database will be created from scratch" >&2
fi

key_files=(config.json node_key.json)
if [ "$IS_TRAFFIC" != 1 ]; then
    key_files+=(validator_key.json)
fi
for f in "${key_files[@]}"; do
    mv "$tmp_home/$f" "$NEAR_HOME/$f"
done

log_filter='client=debug,chain=debug,mirror=debug,mio=warn,tokio_util=warn,info'
jq -n --arg f "$log_filter" '{opentelemetry: $f, rust_log: $f}' >"$NEAR_HOME/log_config.json"

echo awaiting-network-init >"$STATE_FILE"

validator_key=/dev/null
if [ -f "$NEAR_HOME/validator_key.json" ]; then
    validator_key="$NEAR_HOME/validator_key.json"
fi
jq -n -c \
    --slurpfile node_key "$NEAR_HOME/node_key.json" \
    --slurpfile validator_key "$validator_key" \
    --slurpfile config "$NEAR_HOME/config.json" \
    '{
        node_key: $node_key[0].public_key,
        validator_account_id: $validator_key[0].account_id,
        validator_public_key: $validator_key[0].public_key,
        listen_port: ($config[0].network.addr | split(":")[1])
    }'
