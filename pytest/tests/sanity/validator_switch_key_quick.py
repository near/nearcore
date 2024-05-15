#!/usr/bin/env python3
# Starts two validating nodes and one non-validating node
# Set a new validator key that has the same account id as one of
# the validating nodes. Stake that account with the new key
# and make sure that the network doesn't stall even after
# the non-validating node becomes a validator.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster

EPOCH_LENGTH = 30
TIMEOUT = 200

client_config = {
    "tracked_shards": [0],  # Track all shards
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True
}
nodes = start_cluster(
    2, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {
         0: client_config,
         1: client_config
     })
time.sleep(2)


nodes[2].kill()
nodes[2].put_validator_key(nodes[1].validator_key)
#nodes[2].reset_data()
nodes[2].start(boot_node=nodes[0])
time.sleep(3)

block = nodes[0].get_latest_block()
target_height = block.height + 4 * EPOCH_LENGTH

start_time = time.time()
while True:
    assert time.time() - start_time < TIMEOUT, 'Validators got stuck'
    node1_height = nodes[1].get_latest_block().height
    node2_height = nodes[2].get_latest_block().height
    if node1_height > target_height and node2_height > target_height:
        break
    time.sleep(1)

nodes[1].kill()
nodes[2].load_validator_key()

block = nodes[0].get_latest_block()
target_height = block.height + 2 * EPOCH_LENGTH

while True:
    assert time.time() - start_time < TIMEOUT, 'Validators got stuck'
    node1_height = nodes[1].get_latest_block().height
    node2_height = nodes[2].get_latest_block().height
    if node1_height > target_height and node2_height > target_height:
        break
    info = nodes[0].json_rpc('validators', 'latest')
    count = len(info['result']['next_validators'])
    assert count == 2, 'Number of validators do not match'
    validator = info['result']['next_validators'][1]['account_id']
    assert validator == 'test2'
    statuses = sorted((enumerate(node.get_latest_block() for node in [nodes[0], nodes[2]])),
                      key=lambda element: element[1].height)
    last = statuses.pop()
    cur_height = last[1].height
    node = nodes[last[0]]
    for _, block in statuses:
        try:
            node.get_block(block.hash)
        except Exception:
            assert False, 'Nodes are not synced'
