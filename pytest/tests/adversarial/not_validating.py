#!/usr/bin/env python3
# Spins up 4 validating nodes. Wait until the third epoch ends.
# Direct validators to not validate chunks for 1 epoch.
# Let validators validate again, for 2 more epochs.

import logging
import pathlib
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
import state_sync_lib

EPOCH_LENGTH = 10
NUM_VALIDATORS = 4


def wait_until(nodes, target_height):
    last_height = -1
    while True:
        statuses = sorted((enumerate(node.get_latest_block() for node in nodes)),
                        key=lambda element: element[1].height)
        last = statuses.pop()
        height = last[1].height
        if height > target_height:
            break
        if height != last_height:
            logging.info(
                f'@{height}, epoch_height: {state_sync_lib.approximate_epoch_height(height, EPOCH_LENGTH)}'
            )
            last_height = height
        node = nodes[last[0]]
        success = True
        for _, block in statuses:
            try:
                node.get_block(block.hash)
            except Exception:
                success = True
                break
        if not success:
            return False
        time.sleep(0.25)
    return True

def main():
    node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair()
    node_config_sync["tracked_shards"] = []
    node_config_sync["store.load_mem_tries_for_tracked_shards"] = True
    configs = {x: node_config_sync for x in range(NUM_VALIDATORS)}
    configs[NUM_VALIDATORS] = node_config_dump

    nodes = start_cluster(NUM_VALIDATORS, 1, NUM_VALIDATORS, None, [["epoch_length", EPOCH_LENGTH],
                                                                    ["shuffle_shard_assignment_for_chunk_producers", True],
                                                                    ["block_producer_kickout_threshold", 20],
                                                                    ["chunk_producer_kickout_threshold", 20]], configs)

    for node in nodes:
        node.stop_checking_store()

    print("nodes started")
    assert wait_until(nodes, EPOCH_LENGTH * 3)

    for i in range(NUM_VALIDATORS):
        res = nodes[i].json_rpc('adv_disable_chunk_validation', True)
        assert 'result' in res, res
    
    print("disabled chunk validation")
    assert wait_until(nodes, EPOCH_LENGTH * 4)

    for i in range(NUM_VALIDATORS):
        res = nodes[i].json_rpc('adv_disable_chunk_validation', False)
        assert 'result' in res, res

    print("enabled chunk validation back")
    assert wait_until(nodes, EPOCH_LENGTH * 6)

    logging.info("Success")

if __name__ == "__main__":
    main()
