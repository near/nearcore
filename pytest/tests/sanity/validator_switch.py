#!/usr/bin/env python3
# Starts three validating nodes and one non-validating node
# Make the validating nodes unstake and the non-validating node stake
# so that the next epoch block producers set is completely different
# Make sure all nodes can still sync.

import sys, time, base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_staking_tx
import utils

EPOCH_LENGTH = 20
tracked_shards = {
    "tracked_shards": [0],  # Track all shards
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True
}

nodes = start_cluster(
    3, 1, 4, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {
         0: tracked_shards,
         1: tracked_shards
     })

time.sleep(3)

hash_ = nodes[0].get_latest_block().hash_bytes

for i in range(4):
    stake = 50000000000000000000000000000000 if i == 3 else 0
    tx = sign_staking_tx(nodes[i].signer_key, nodes[i].validator_key, stake, 1,
                         hash_)
    nodes[0].send_tx(tx)
    logger.info("test%s stakes %d" % (i, stake))

for cur_height, _ in utils.poll_blocks(nodes[0], poll_interval=1):
    if cur_height >= EPOCH_LENGTH * 2:
        break
    if cur_height > EPOCH_LENGTH + 1:
        info = nodes[0].json_rpc('validators', 'latest')
        count = len(info['result']['next_validators'])
        assert count == 1, 'Number of validators do not match'
        validator = info['result']['next_validators'][0]['account_id']
        assert validator == 'test3'

while cur_height <= EPOCH_LENGTH * 3:
    statuses = sorted((enumerate(node.get_latest_block() for node in nodes)),
                      key=lambda element: element[1].height)
    last = statuses.pop()
    cur_height = last[1].height
    node = nodes[last[0]]
    succeed = True
    for _, block in statuses:
        try:
            node.get_block(block.hash)
        except Exception:
            succeed = False
            break
    if statuses[0][1].height > EPOCH_LENGTH * 2 + 5 and succeed:
        sys.exit(0)

assert False, 'Nodes are not synced'
