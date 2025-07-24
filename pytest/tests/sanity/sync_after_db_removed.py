#!/usr/bin/env python3
# Spins up three validating nodes. Stop one of them and make another one produce
# sufficient number of blocks. Restart the stopped node and check that it can
# still sync. Then check all old data is removed.

import pathlib
import sys
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import state_sync_lib
import utils

EPOCH_LENGTH = 15
TARGET_HEIGHT = int(EPOCH_LENGTH * 6)
AFTER_SYNC_HEIGHT = EPOCH_LENGTH * 10

class EpochSyncTest(unittest.TestCase):

    def setUp(self):
        pass

    def test(self):
        self.node_config = state_sync_lib.get_state_sync_config_p2p(None)
        # We're generating many blocks here - lower the min production delay to speed
        # up the test running time a little.
        self.node_config["consensus.min_block_production_delay"] = {
            "secs": 0,
            "nanos": 100000000
        }
        self.node_config["consensus.max_block_production_delay"] = {
            "secs": 0,
            "nanos": 300000000
        }
        self.node_config["consensus.max_block_wait_delay"] = {"secs": 0, "nanos": 600000000}
        self.node_config["consensus.block_fetch_horizon"] = 1
        self.node_config["gc_step_period"] = {"secs": 0, "nanos": 100000000}
        self.node_config["gc_blocks_limit"] = 3

        self.nodes = start_cluster(
            num_nodes=4,
            num_observers=0,
            num_shards=4,
            config=None,
            genesis_config_changes=[["epoch_length", EPOCH_LENGTH],
                   ["num_block_producer_seats_per_shard", [1, 1, 1, 1]],
                   ["validators", 0, "amount", "60000000000000000000000000000000"],
                   ["block_producer_kickout_threshold", 50],
                   ["chunk_producer_kickout_threshold", 50],
                   [
                       "records", 0, "Account", "account", "locked",
                       "60000000000000000000000000000000"
                   ], ["total_supply", "5010000000000000000000000000000000"]],
            client_config_changes={x: self.node_config for x in range(4)})
        
        node0_height, _ = utils.wait_for_blocks(self.nodes[0], target=TARGET_HEIGHT)

        logger.info('Kill node 1')
        self.nodes[1].kill()
        
  
        # remove the state from node 1 here

        logger.info('Restart node 1')
        self.nodes[1].start(boot_node=self.nodes[0])
        
        node1_height, _ = utils.wait_for_blocks(self.nodes[1], target=TARGET_HEIGHT + EPOCH_LENGTH + 1)

if __name__ == '__main__':
    unittest.main()

