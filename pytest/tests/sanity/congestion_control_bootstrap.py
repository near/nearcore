#!/usr/bin/env python3

import unittest
import pathlib
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import utils

EPOCH_LENGTH = 5
NUM_SHARDS = 4


class CongestionControlBootstrapTest(unittest.TestCase):
    """Tests congestion control bootstrapping in the case of node restart.
     
    It runs the chain multiple epochs so that garbage collection kicks in.
    Then it restarts the nodes to ensure that congestion control bootstrapping does not fail."""

    def test(self):
        self.start_nodes()
        self.check_congestion_info()
        # Now wait until there are enough epocks to pass
        # for GC to kick in and then restart the nodes.
        self.wait_for_multiple_epochs()
        self.restart_nodes()
        self.check_congestion_info()

    def start_nodes(self):
        self.nodes = cluster.start_cluster(
            num_nodes=4,
            num_observers=0,
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[["epoch_length", EPOCH_LENGTH],
                                    ["block_producer_kickout_threshold", 0],
                                    ["chunk_producer_kickout_threshold", 0]],
            client_config_changes={
                i: {
                    "tracked_shards": [0],
                    "gc_num_epochs_to_keep": 2
                } for i in range(4)
            })
        utils.wait_for_blocks(self.nodes[0], count=3)

    def wait_for_multiple_epochs(self):
        utils.wait_for_blocks(self.nodes[0], count=5 * EPOCH_LENGTH)

    def restart_nodes(self):
        for i in range(len(self.nodes)):
            self.nodes[i].kill()
            time.sleep(2)
            self.nodes[i].start(boot_node=None if i == 0 else self.nodes[0])

    def check_congestion_info(self):
        (_, block_hash) = self.nodes[0].get_latest_block()
        for s in range(NUM_SHARDS):
            result = self.nodes[0].json_rpc("chunk", {
                "block_id": block_hash,
                "shard_id": s
            })
            self.assertIn('result', result, result)
            chunk = result['result']
            congestion_info = chunk['header']['congestion_info']
            self.assertEqual(int(congestion_info['buffered_receipts_gas']), 0)
            self.assertEqual(int(congestion_info['delayed_receipts_gas']), 0)
            self.assertEqual(congestion_info['receipt_bytes'], 0)


if __name__ == '__main__':
    unittest.main()
