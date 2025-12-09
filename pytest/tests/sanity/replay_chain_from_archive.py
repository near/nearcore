#!/usr/bin/env python3
# Spins up 2 validating nodes, 1 non-validating RPC node, and 1 non-validating archival node.
# There are four shards in this test.
# Issues a number of transactions on the chain, and then replays the chain from the archival node.

import copy
import pathlib
import subprocess
import sys
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import simple_test
import state_sync_lib
from geventhttpclient import useragent

NUM_VALIDATORS = 2
EPOCH_LENGTH = 10
GC_NUM_EPOCHS_TO_KEEP = 3
GC_BLOCKS_LIMIT = 5

# Shard layout with 5 roughly equal size shards for convenience.
SHARD_LAYOUT = {
    "V1": {
        "boundary_accounts": [
            "fff",
            "kkk",
            "ppp",
            "uuu",
        ],
        "version": 2,
        "shards_split_map": None,
        "to_parent_shard_map": None,
    }
}

NUM_SHARDS = len(SHARD_LAYOUT["V1"]["boundary_accounts"]) + 1

ALL_ACCOUNTS_PREFIXES = [
    "aaa",
    "ggg",
    "lll",
    "rrr",
    "vvv",
]


class ReplayChainFromArchiveTest(unittest.TestCase):

    def setUp(self):
        node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
        )

        # Validator node configs: Enable single-shard tracking with memtries enabled.
        node_config_sync["tracked_shards_config"] = "NoShards"
        configs = {x: node_config_sync for x in range(NUM_VALIDATORS)}

        # Dump+RPC node config: Enable tracking all shards with memtries enabled.
        node_config_dump["tracked_shards_config"] = "AllShards"
        configs[NUM_VALIDATORS] = node_config_dump

        # Archival node config: Enable tracking all shards with memtries enabled.
        node_config_archival = copy.deepcopy(node_config_dump)
        node_config_archival["archive"] = True
        configs[NUM_VALIDATORS + 1] = node_config_archival

        # Configure GC to make sure it runs before the random workload finishes.
        for i in configs.keys():
            configs[i]["gc_num_epochs_to_keep"] = GC_NUM_EPOCHS_TO_KEEP
            configs[i]["gc_blocks_limit"] = GC_BLOCKS_LIMIT
            configs[i]["gc_step_period"] = {"secs": 0, "nanos": 100000000}

        self.nodes = cluster.start_cluster(
            num_nodes=NUM_VALIDATORS,
            num_observers=2,
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[["genesis_height", 0],
                                    ["epoch_length", EPOCH_LENGTH],
                                    ["shard_layout", SHARD_LAYOUT],
                                    ["block_producer_kickout_threshold", 0],
                                    ["chunk_producer_kickout_threshold", 0]],
            client_config_changes=configs)
        self.assertEqual(NUM_VALIDATORS + 2, len(self.nodes))

        self.rpc_node = self.nodes[NUM_VALIDATORS]
        self.archival_node = self.nodes[NUM_VALIDATORS + 1]

    def test(self):
        self.testcase = simple_test.SimpleTransferBetweenAccounts(
            nodes=self.nodes,
            rpc_node=self.rpc_node,
            account_prefixes=ALL_ACCOUNTS_PREFIXES,
            epoch_length=EPOCH_LENGTH)

        self.testcase.wait_for_blocks(5)

        self.testcase.create_accounts()

        self.testcase.deploy_contracts()

        # Run the test until there are enough number of epochs to trigger garbage collection.
        self.testcase.random_workload_until(EPOCH_LENGTH *
                                            (GC_NUM_EPOCHS_TO_KEEP + 2) +
                                            GC_BLOCKS_LIMIT)

        # Sanity check: Validators cannot return old blocks after GC (eg. genesis block) but archival node can.
        logger.info("Running sanity check for archival node")
        for node in self.nodes:
            try:
                result = node.get_block_by_height(GC_BLOCKS_LIMIT - 1)
                if node == self.archival_node:
                    assert "error" not in result, result
                    assert result["result"]["header"][
                        "hash"] is not None, result
                else:
                    assert "error" in result, result
            except useragent.BadStatusCode as e:
                assert "code=422" in str(
                    e), f"Expected status code 422 in exception, got: {e}"
            except Exception as e:
                assert (
                    False
                ), f"Unexpected exception type raised: {type(e)}. Exception: {e}"

        # Capture the last height to replay before killing the nodes.
        end_height = self.testcase.wait_for_blocks(1)

        logger.info("Killing all nodes before replaying the chain")
        for node in self.nodes:
            node.kill()

        logger.info("Replaying the chain using the archival node")
        self.replay_chain(start_height=0, end_height=end_height)

        logger.info("Test ended")

    def replay_chain(self, start_height, end_height):
        cmd = self.archival_node.get_command_for_subprogram(
            ("replay-archive", "--start-height", str(start_height),
             "--end-height", str(end_height)))
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = process.communicate(timeout=60)
        assert 0 == process.returncode, err


if __name__ == '__main__':
    unittest.main()
