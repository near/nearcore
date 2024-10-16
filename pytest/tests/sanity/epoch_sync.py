#!/usr/bin/env python3
# Spins up a node, then waits for 10 epochs.
# Spin up another node with epoch sync enabled, and make sure it catches up.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
import state_sync_lib
import utils

# The time it takes for an epoch needs to be long enough to reliably have
# state dumps. If at some point this test ends up being flaky because node0
# is spitting out state sync dumper errors like "Wrong snapshot hash",
# increase this further.
EPOCH_LENGTH = 15

# We can only do epoch sync if there are enough epochs to begin with, so have
# a few epochs.
SYNC_FROM_BLOCK = 5 * EPOCH_LENGTH
# After epoch sync, let's run for enough epochs for GC to kick in, to verify
# that the node is fine with GC too.
CATCHUP_BLOCK = 12 * EPOCH_LENGTH

config = load_config()

node_config = state_sync_lib.get_state_sync_config_combined()

node_config['epoch_sync'] = {
    "enabled": True,
    "epoch_sync_horizon": EPOCH_LENGTH * 3,
    "epoch_sync_accept_proof_max_horizon": EPOCH_LENGTH * 3,
    "timeout_for_epoch_sync": {
      "secs": 5,
      "nanos": 0
    }
}

# Make sure that state sync targets an epoch *later* than the epoch sync target epoch.
node_config["consensus.block_fetch_horizon"] = 3
node_config["consensus.block_header_fetch_horizon"] = EPOCH_LENGTH

near_root, node_dirs = init_cluster(
    2, 1, 1, config,
    [["min_gas_price", 0], ["epoch_length", EPOCH_LENGTH]],
    {x: node_config for x in range(3)})

node0 = spin_up_node(config, near_root, node_dirs[0], 0)
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=node0)

ctx = utils.TxContext([0, 0], [node0, node1])

for height, block_hash in utils.poll_blocks(node0,
                                            timeout=SYNC_FROM_BLOCK * 2,
                                            poll_interval=0.1):
    if height >= SYNC_FROM_BLOCK:
        break
    ctx.send_moar_txs(block_hash, 1, False)

node1 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=node0)
tracker = utils.LogTracker(node1)

utils.wait_for_blocks(node1, target=CATCHUP_BLOCK, timeout=(CATCHUP_BLOCK - SYNC_FROM_BLOCK) * 2)

# Verify that we did bootstrap using epoch sync (rather than header sync).
tracker.check('Bootstrapped from epoch sync')
