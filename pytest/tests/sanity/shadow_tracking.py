#!/usr/bin/env python3
# Starts two validating nodes, one failover node, and one dumper node.
# Set the failover node to shadow track one of the validators.
# Stop the failover node for 1 epoch during which shard assignment changes.
# Restart the failover node and wait for state sync to finish.
# Ensure the failover node has chunks for the shards it supposed to track as shadow validator.
# Wait for 1 epoch so that shard assignment changes and do the check again, repeat 3 times.

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import start_cluster
import state_sync_lib
from utils import wait_for_blocks

EPOCH_LENGTH = 10
TIMEOUT = 100


class ShadowTrackingTest(unittest.TestCase):

    def _get_final_block_height(self, nodes):
        height_per_node = [node.get_latest_block().height for node in nodes]
        min_height = min(height_per_node)
        max_height = max(height_per_node)
        self.assertGreaterEqual(min_height + 1, max_height, height_per_node)
        return min_height

    def _get_block_hash(self, block_height, node):
        result = node.get_block_by_height(block_height)
        self.assertNotIn('error', result, result)
        self.assertIn('result', result, result)
        return result['result']['header']['hash']

    def _get_shard_assignment(self, rpc_node):
        result = rpc_node.json_rpc('validators', 'latest')
        self.assertNotIn('error', result, result)
        self.assertIn('result', result, result)
        validators = result['result']['current_validators']
        shard_assigment = {}
        for validator in validators:
            shard_assigment[validator['account_id']] = validator['shards']
        return shard_assigment

    def _has_chunk(self, block_hash, shard_id, node):
        result = node.json_rpc("chunk", {
            "block_id": block_hash,
            "shard_id": shard_id
        })
        if 'error' in result:
            return False
        self.assertIn('result', result, result)
        return True

    def test_shadow_tracking(self):
        node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
        )
        node_config_sync["tracked_shards"] = []
        node_config_sync["store.load_mem_tries_for_tracked_shards"] = True
        configs = {x: node_config_sync for x in range(3)}
        configs[3] = node_config_dump

        # Set the failover node to shadow track "test0".
        configs[2]["tracked_shadow_validator"] = "test0"

        nodes = start_cluster(
            2, 2, 3, None,
            [["epoch_length", EPOCH_LENGTH],
             ["shuffle_shard_assignment_for_chunk_producers", True],
             ["block_producer_kickout_threshold", 20],
             ["chunk_producer_kickout_threshold", 20]], configs)

        for node in nodes:
            node.stop_checking_store()

        # Wait for 1 epoch so that shard shuffling kicks in.
        wait_for_blocks(nodes[3], count=EPOCH_LENGTH)
        logger.info('## Initial shard assignment: {}'.format(
            self._get_shard_assignment(nodes[3])))

        # Stop the failover node for 1 epoch, so that it has to state sync to a new shard tracked by "test0".
        nodes[2].kill()
        wait_for_blocks(nodes[3], count=EPOCH_LENGTH)
        nodes[2].start(boot_node=nodes[3])
        # Give it some time to catch up.
        wait_for_blocks(nodes[3], count=EPOCH_LENGTH)

        round = 0
        while True:
            round += 1
            shards = self._get_shard_assignment(nodes[3])
            logger.info(f'## Round {round} shard assigment: {shards}')
            block_height = self._get_final_block_height(nodes)
            block_hash = self._get_block_hash(block_height, nodes[3])
            for shard in shards['test0']:
                # The RPC node should have chunk from a shard tracked by "test0".
                self.assertTrue(self._has_chunk(block_hash, shard, nodes[2]))
            if round == 3:
                break
            wait_for_blocks(nodes[3], count=EPOCH_LENGTH)


if __name__ == '__main__':
    unittest.main()
