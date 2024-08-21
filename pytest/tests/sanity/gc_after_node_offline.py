#!/usr/bin/env python3
# Spins up 4 validating nodes. Let one of them (node0) tracks shard 0 only.
# Stop node0, wait for 2 epochs, and restart it.
# Eventually, blocks from before the restart should be garbage collected.

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
import state_sync_lib
from utils import wait_for_blocks

EPOCH_LENGTH = 10
# Should only keep last 30 blocks.
NUM_GC_EPOCHS = 3


class GcAfterNodeOffline(unittest.TestCase):

    def _prepare_cluster(self):
        node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
        )
        node_config_sync["tracked_shards"] = []
        node_config_sync["gc_num_epochs_to_keep"] = NUM_GC_EPOCHS
        configs = {x: node_config_sync for x in range(4)}
        configs[4] = node_config_dump
        # Let node0 tracks shard 0 only, regardless of their assignment.
        configs[0]['tracked_shard_schedule'] = [[0]]

        nodes = start_cluster(4, 1, 2, None,
                              [["epoch_length", EPOCH_LENGTH],
                               ["block_producer_kickout_threshold", 0],
                               ["chunk_producer_kickout_threshold", 0]],
                              configs)

        for node in nodes:
            node.stop_checking_store()

        node0 = nodes[0]
        rpc_node = nodes[-1]
        return node0, rpc_node

    def _has_block(self, node, block_height):
        result = node.json_rpc('block', [block_height], timeout=10)
        if 'error' in result:
            return False, result
        self.assertIn('result', result, result)
        return True, result

    def test_gc_after_node_offline(self):
        node0, rpc_node = self._prepare_cluster()

        wait_for_blocks(rpc_node, target=20)
        node0.kill()
        wait_for_blocks(rpc_node, target=40)
        node0.start(boot_node=node0)
        wait_for_blocks(rpc_node, target=60)

        # Old data is garbage collected.
        for i in range(1, 28):
            has_block, res = self._has_block(node0, i)
            self.assertFalse(has_block, f'height {i}: {res}')


if __name__ == '__main__':
    unittest.main()
