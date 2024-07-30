#!/usr/bin/env python3
# Spins up 4 validating nodes, 1 non-validating RPC node, and 1 non-validating archival node.
# There are four shards in this test.
# Issues a number of transactions on the chain, and then replays the chain from the archival node.

import unittest
import pathlib
import subprocess
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import simple_test

EPOCH_LENGTH = 10
GC_NUM_EPOCHS_TO_KEEP = 3

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
        "shards_split_map": [],
        "to_parent_shard_map": [],
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
        # Validator node configs: Enable single-shard tracking with memtries enabled.
        validator_config = {}
        validator_config["tracked_shards"] = []
        configs = {x: validator_config for x in range(4)}

        # RPC node config: Enable tracking all shards with memtries enabled.
        rpc_config = {}
        rpc_config["tracked_shards"] = [0]
        configs[4] = rpc_config

        # Archival node config: Enable tracking all shared with memtries enabled.
        archival_config = {}
        archival_config["archive"] = True
        archival_config["tracked_shards"] = [0]
        configs[5] = archival_config

        for i in configs.keys():
            configs[i]["gc_num_epochs_to_keep"] = GC_NUM_EPOCHS_TO_KEEP

        self.nodes = cluster.start_cluster(
            num_nodes=4,
            num_observers=2,
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[
                ["epoch_length", EPOCH_LENGTH], ["shard_layout", SHARD_LAYOUT],
                ["shuffle_shard_assignment_for_chunk_producers", True],
                ["block_producer_kickout_threshold", 0],
                ["chunk_producer_kickout_threshold", 0]
            ],
            client_config_changes=configs)
        self.assertEqual(6, len(self.nodes))

        self.rpc_node = self.nodes[4]
        self.archival_node = self.nodes[5]

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
                                            (GC_NUM_EPOCHS_TO_KEEP + 2) + 5)

        # self.testcase.wait_for_txs(assert_all_accepted=False)

        # # Sanity check: Validators cannot return old blocks after GC (eg. genesis block) but archival node can.
        # for node in self.nodes:
        #     if node == self.archival_node:
        #         result = self.nodes[5].get_block_by_height(3)
        #         assert 'error' not in result, result
        #         assert result['result']['header']['hash'] is not None, result
        #     else:
        #         result = self.nodes[i].get_block_by_height(3)
        #         assert 'error' in result, result

        # logger.info("Killing all nodes before replaying the chain")
        # for i in len(self.nodes):
        #     self.nodes[i].kill()

        # logger.info("Replaying the chain")
        # self.replay_chain()

        logger.info("Test ended")

    def replay_chain(self):
        cmd = self.archival_node.get_command_for_subprogram(("replay-archive"))
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out, err = process.communicate()
        assert 0 == process.returncode, err


if __name__ == '__main__':
    unittest.main()
