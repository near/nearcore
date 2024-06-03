#!/usr/bin/env python3
# Starts three validating nodes and one non-validating node
# Set a new validator key that has the same account id as one of
# the validating nodes. Stake that account with the new key
# and make sure that the network doesn't stall even after
# the non-validating node becomes a validator.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster

EPOCH_LENGTH = 20
TIMEOUT = 100
NUM_VALIDATORS = 2

client_config = {
    "tracked_shards": [0],  # Track all shards
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True
}
config_map = {i: client_config for i in range(NUM_VALIDATORS)}
nodes = start_cluster(
    NUM_VALIDATORS, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], config_map)
time.sleep(2)

nodes[NUM_VALIDATORS].reset_validator_key(nodes[0].validator_key)
nodes[0].kill()
nodes[NUM_VALIDATORS].reload_updateable_config()
nodes[NUM_VALIDATORS].stop_checking_store()
time.sleep(2)

block = nodes[1].get_latest_block()
target_height = block.height + 4 * EPOCH_LENGTH
start_time = time.time()

while True:
    assert time.time() - start_time < TIMEOUT, 'Validators got stuck'
    old_validator_height = nodes[1].get_latest_block().height
    new_validator_height = nodes[NUM_VALIDATORS].get_latest_block().height
    if old_validator_height > target_height and new_validator_height > target_height:
        break
    info = nodes[1].json_rpc('validators', 'latest')
    count = len(info['result']['next_validators'])
    assert count == NUM_VALIDATORS, 'Number of validators do not match'
    validator = info['result']['next_validators'][0]['account_id']
    # We copied over 'test0' validator key, along with validator account ID.
    # Therefore, despite nodes[0] being stopped, 'test0' still figures as active validator.
    assert validator == 'test0'
    statuses = sorted([(node_idx, nodes[node_idx].get_latest_block()) for node_idx in range(1, NUM_VALIDATORS + 1)],
                      key=lambda element: element[1].height)
    print(statuses)
    last = statuses.pop()
    cur_height = last[1].height
    node = nodes[last[0]]
    succeed = True
    for _, block in statuses:
        try:
            print(block.hash)
            node.get_block(block.hash)
        except Exception:
            succeed = False
            break
    print(succeed)
    # if statuses[0][1].height > EPOCH_LENGTH * 2 + 5 and succeed:
    #     sys.exit(0)
    time.sleep(1)

assert False, 'Nodes are not synced'
