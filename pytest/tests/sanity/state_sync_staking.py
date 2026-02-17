#!/usr/bin/env python3
"""
Test: a new validator joins via staking after state-syncing.

Scenario:
  1. Genesis has 4 validators (test0-test3) + test4 account with balance (unstaked).
  2. Start 4 validator nodes; node4 (test4) is NOT started yet.
  3. Wait past the block fetch horizon and a couple of epochs so that any
     late-joining node will have to state sync.
  4. Send a stake transaction for test4, then start node4 with NoShards tracking.
  5. After the epoch boundary, verify test4 has joined the validator set and
     node4 has caught up.
"""

import json
import pathlib
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from key import Key
from transaction import sign_staking_tx
import state_sync_lib
import utils

EPOCH_LENGTH = 20
TIMEOUT = 600

# --- Configure state sync ---
# Validators use NoShards (only track assigned shards, production-like).
# Node0 also dumps state parts for others to sync from.
(node_config_dump, node_config_sync) = state_sync_lib.get_state_sync_configs_pair()

node_config_dump["tracked_shards_config"] = "NoShards"
node_config_dump["consensus"] = {"block_fetch_horizon": 5}

node_config_sync["tracked_shards_config"] = "NoShards"
node_config_sync["consensus"] = {"block_fetch_horizon": 5}

# Node0 dumps state; nodes 1-4 sync from it.
# Node4 (test4, unstaked) also uses NoShards — it will state sync when started.
configs = {i: node_config_sync for i in range(5)}
configs[0] = node_config_dump  # node0 dumps state parts

config = load_config()

# 4 validators + 1 observer (test4 has balance but no initial stake).
near_root, node_dirs = init_cluster(
    4, 1, 1, config,
    [
        ["epoch_length", EPOCH_LENGTH],
        ["block_producer_kickout_threshold", 40],
        ["chunk_producer_kickout_threshold", 40],
    ],
    configs,
)

# Start only the 4 validator nodes (test0-test3)
boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
nodes = [boot_node]
for i in range(1, 4):
    node = spin_up_node(config, near_root, node_dirs[i], i, boot_node=boot_node)
    nodes.append(node)

# for node in nodes:
#     node.stop_checking_store()

logger.info("4 validator nodes started (test0-test3)")

# Wait for block fetch horizon + a couple of epochs
target_height = EPOCH_LENGTH * 3 + 5
cur_height, _ = utils.wait_for_blocks(boot_node, target=target_height, timeout=TIMEOUT)
logger.info(f"Chain progressed to height {cur_height}")

def get_validators():
    """Returns the set of current validator account IDs from the validators RPC"""
    # return set(x['account_id'] for x in boot_node.get_status()['validators'])
    info = boot_node.json_rpc('validators', [None])
    return set(v['account_id'] for v in info['result']['current_validators'])

def get_validator_info():
    """Get detailed validator information from RPC"""
    return boot_node.json_rpc('validators', [None])


# Send stake tx for test4, THEN start node4
initial_validators = get_validators()
logger.info(f"Current validators before staking: {sorted(initial_validators)}")
assert 'test4' not in initial_validators, "test4 should not be a validator initially"

# Read test4's keys from disk (node4 is not started yet).
node4_dir = node_dirs[4]
with open(f'{node4_dir}/validator_key.json') as f:
    vk = json.load(f)
    node4_validator_key = Key(vk['account_id'], vk['public_key'],
                              vk['secret_key'])
node4_signer_key = node4_validator_key

nonce = 0
try:
    access_key_response = boot_node.get_access_key_list("test4")
    for key in access_key_response['result']['keys']:
        nonce = max(nonce, key['access_key']['nonce'])
    nonce += 1
    logger.info(f"Using nonce {nonce} for test4 stake transaction")
except Exception as e:
    logger.warning(f"Could not get nonce from access key: {e}, using nonce 1")
    nonce = 1

# Send staking transaction for test4.
stake_amount = 50000000000000000000000000000000  # 50k NEAR
block_hash = boot_node.get_latest_block().hash_bytes

tx = sign_staking_tx(
    node4_signer_key,
    node4_validator_key,
    stake_amount,
    nonce,
    block_hash,
)
res = boot_node.send_tx_and_wait(tx, timeout=15)
assert 'result' in res and 'error' not in res, f"Staking tx failed: {res}"
logger.info("Staking transaction for test4 sent and confirmed")

# Check proposals/next_validators after staking.
validator_info = get_validator_info()
proposals = [v['account_id']
             for v in validator_info['result']['current_proposals']]
next_validators = [v['account_id']
             for v in validator_info['result']['next_validators']]
logger.info(f"After staking — proposals: {proposals}, next_validators: {next_validators}")

if 'test4' in proposals:
    logger.info("test4 found in current_proposals — stake accepted")
elif 'test4' in next_validators:
    logger.info("test4 found in next_validators — already scheduled")
else:
    logger.warning("test4 NOT in proposals or next_validators yet — "
                   "may appear at next epoch boundary")

# start node4 — it will state sync to catch up.
node4 = spin_up_node(config,
                      near_root,
                      node_dirs[4],
                      4,
                      boot_node=boot_node)
node4.stop_checking_store()
nodes.append(node4)
logger.info("Node4 (test4) started with NoShards — will state sync to catch up")

# Wait for test4 to join the validator set.
# Stake takes effect ~2 epochs after inclusion. We staked around epoch 3,
# so test4 should appear in the validator set by epoch 5-6.
last_log_height = 0
for height, _ in utils.poll_blocks(boot_node, timeout=TIMEOUT):
    # Log progress every epoch.
    if height - last_log_height >= EPOCH_LENGTH:
        current_validators = get_validators()
        info = get_validator_info()
        props = [v['account_id']
                 for v in info['result']['current_proposals']]
        nxt = [v['account_id']
               for v in info['result']['next_validators']]
        logger.info(f"Height {height}: validators={sorted(current_validators)} "
                    f"proposals={props} next={nxt}")
        last_log_height = height

    if 'test4' in get_validators():
        logger.info(f"test4 joined validator set at height {height}!")
        break
else:
    assert False, "test4 never joined the validator set"

# Wait a few more blocks for test4 to produce.
time.sleep(10)

# Log test4 production stats.
final_info = get_validator_info()
for v in final_info['result']['current_validators']:
    if v['account_id'] == 'test4':
        logger.info(f"test4 production — "
                    f"blocks: {v['num_produced_blocks']}/{v['num_expected_blocks']}, "
                    f"chunks: {v['num_produced_chunks']}/{v['num_expected_chunks']}, "
                    f"endorsements: {v['num_produced_endorsements']}/{v['num_expected_endorsements']}")
        break

# Verify node4 has caught up reasonably close to the boot node.
boot_height = boot_node.get_latest_block().height
node4_height = node4.get_latest_block().height
logger.info(f"boot_node at {boot_height}, node4 at {node4_height}")
assert node4_height + EPOCH_LENGTH >= boot_height, (
    f"node4 ({node4_height}) too far behind boot_node ({boot_height})")

logger.info("SUCCESS: test4 joined validators after state sync")
