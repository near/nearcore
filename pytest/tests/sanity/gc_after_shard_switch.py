#!/usr/bin/env python3
# Spins up 4 validating nodes. Let one of them (node0) tracks shard 0 only.
# Stop node0, wait a bit, and restart it but this time tracking shard 1 only.
# Node0 should eventually garbage collect all shard 0 data.

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
import state_sync_lib
from utils import wait_for_blocks


EPOCH_LENGTH = 10
NUM_GC_EPOCHS = 3
NUM_BLOCKS_TO_KEEP = EPOCH_LENGTH * NUM_GC_EPOCHS 


class GcAfterShardSwitch(unittest.TestCase):

    def _prepare_cluster(self):
        node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
        )
        node_config_sync["tracked_shards"] = []
        node_config_sync["gc_num_epochs_to_keep"] = NUM_GC_EPOCHS
        configs = {x: node_config_sync for x in range(4)}
        configs[4] = node_config_dump
        # Let node0 tracks shard 0 only, regardless of their assignment.
        configs[0]['tracked_shard_schedule'] = [[0]]

        nodes = start_cluster(
            4, 1, 2, None,
            [["epoch_length", EPOCH_LENGTH],
             ["block_producer_kickout_threshold", 0],
             ["chunk_producer_kickout_threshold", 0]], configs)

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
    
    # A wrapper for `wait_for_blocks` function that saves info about recent blocks hashes.
    def _wait_for_blocks(self, node, block_height_to_hash, target):
        wait_for_blocks(node, target=target)
        start_block = max(1, target - NUM_BLOCKS_TO_KEEP)
        for height in range(start_block, target):
            has_block, result = self._has_block(node, height)
            if has_block:
                block_hash = result['result']['header']['hash']
                block_height_to_hash[height] = block_hash

    def _has_chunk(self, node, shard_id, block_hash):
        result = node.json_rpc("chunk", {
            "block_id": block_hash,
            "shard_id": shard_id
        })
        if 'error' in result:
            return False, result
        self.assertIn('result', result, result)
        return True, result


    def test_gc_after_shard_switch(self):
        node0, rpc_node = self._prepare_cluster()
        block_height_to_hash = {}

        self._wait_for_blocks(rpc_node, block_height_to_hash, target=20)
        node0.kill()
        # Let node0 now tracks shard 1 only.
        node0.change_config({'tracked_shard_schedule': [[1]]})
        node0.start(boot_node=node0)
        self._wait_for_blocks(rpc_node, block_height_to_hash, target=40)
        self._wait_for_blocks(rpc_node, block_height_to_hash, target=60)

        # Most of the blocks should be known.
        self.assertGreater(len(block_height_to_hash), 45, block_height_to_hash.keys())

        # New data is not garbage collected.
        for height in range(31, 60):
            if height not in block_height_to_hash:
                continue
            has_block, res = self._has_block(node0, height)
            self.assertTrue(has_block, f'height {height}: {res}')
            
            has_chunk, res = self._has_chunk(node0, 1, height)
            self.assertTrue(has_chunk, f'height {height}, shard 1: {res}')

            # But we should not have chunks for shard 0 that we no longer track
            has_chunk, res = self._has_chunk(node0, 0, height)
            self.assertFalse(has_chunk, f'height {height}, shard 0: {res}')

        # Old data is garbage collected.
        for height in range(1, 28):
            if height not in block_height_to_hash:
                continue
            has_block, res = self._has_block(node0, height)
            self.assertFalse(has_block, f'height {height}: {res}')
            
            for shard_id in [0, 1]:
                has_chunk, res = self._has_chunk(node0, shard_id, height)
                self.assertFalse(has_chunk, f'height {height}, shard {shard_id}: {res}')
        


if __name__ == '__main__':
    unittest.main()
