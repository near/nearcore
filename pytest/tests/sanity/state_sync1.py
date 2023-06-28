#!/usr/bin/env python3
# Spins up two out of three validating nodes. Waits until they reach height 40.
# Start the last validating node and check that the second node can sync up before
# the end of epoch and produce blocks and chunks.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

BLOCK_WAIT = 40
EPOCH_LENGTH = 80

consensus_config = {
    "consensus": {
        "block_fetch_horizon": 10,
        "block_header_fetch_horizon": 10
    },
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}
node_config = {
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}

nodes = start_cluster(
    4, 0, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {
         0: consensus_config,
         1: consensus_config,
         2: node_config,
         3: node_config
     })
time.sleep(2)
nodes[1].kill()

logger.info("step 1")
utils.wait_for_blocks(nodes[0], target=BLOCK_WAIT)
nodes[1].start(boot_node=nodes[1])
time.sleep(2)

logger.info("step 2")
synced = False
block_height0 = block_height1 = -1
while block_height0 <= EPOCH_LENGTH and block_height1 <= EPOCH_LENGTH:
    block_height0, block_hash0 = nodes[0].get_latest_block()
    block_height1, block_hash1 = nodes[1].get_latest_block()
    if block_height0 > BLOCK_WAIT:
        if block_height0 > block_height1:
            try:
                nodes[0].get_block(block_hash1)
                if synced and abs(block_height0 - block_height1) >= 5:
                    assert False, "Nodes fall out of sync"
                synced = abs(block_height0 - block_height1) < 5
            except Exception:
                pass
        else:
            try:
                nodes[1].get_block(block_hash0)
                if synced and abs(block_height0 - block_height1) >= 5:
                    assert False, "Nodes fall out of sync"
                synced = abs(block_height0 - block_height1) < 5
            except Exception:
                pass
    time.sleep(1)

if not synced:
    assert False, "Nodes are not synced"

validator_info = nodes[0].json_rpc('validators', 'latest')
if len(validator_info['result']['next_validators']) < 2:
    assert False, "Node 1 did not produce enough blocks"

for i in range(2):
    account0 = nodes[0].get_account("test%s" % i)['result']
    account1 = nodes[1].get_account("test%s" % i)['result']
    print(account0, account1)
    assert account0 == account1, "state diverged"
