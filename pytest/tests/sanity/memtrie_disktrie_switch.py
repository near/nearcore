#!/usr/bin/env python3
# Spins up 2 validating nodes and 1 non-validating node. There are four shards in this test.
# Tests the following scenario and checks if the network can progress over a few epochs.
# 1. Starts with memtries enabled.
# 2. Restarts the validator nodes with memtries disabled.
# 3. Restarts the validator nodes with memtries enabled again.
# Sends random transactions between shards at each step.

import pathlib
import random
import sys
import time
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import state_sync_lib
import simple_test

NUM_VALIDATORS = 2
EPOCH_LENGTH = 10

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


class MemtrieDiskTrieSwitchTest(unittest.TestCase):

    def setUp(self):
        node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
        )

        # Validator node configs: Enable single-shard tracking with memtries enabled.
        node_config_sync["tracked_shards"] = []
        node_config_sync["store.load_mem_tries_for_tracked_shards"] = True
        configs = {x: node_config_sync for x in range(NUM_VALIDATORS)}

        # Dumper node config: Enable tracking all shards with memtries enabled.
        node_config_dump["tracked_shards"] = [0]
        node_config_dump["store.load_mem_tries_for_tracked_shards"] = True
        configs[NUM_VALIDATORS] = node_config_dump

        self.nodes = cluster.start_cluster(
            num_nodes=NUM_VALIDATORS,
            num_observers=1,
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[
                ["epoch_length", EPOCH_LENGTH], ["shard_layout", SHARD_LAYOUT],
                ["shuffle_shard_assignment_for_chunk_producers", True],
                ["block_producer_kickout_threshold", 0],
                ["chunk_producer_kickout_threshold", 0]
            ],
            client_config_changes=configs)
        self.assertEqual(NUM_VALIDATORS + 1, len(self.nodes))

        self.rpc_node = self.nodes[NUM_VALIDATORS]

    def test(self):
        self.testcase = simple_test.SimpleTransferBetweenAccounts(
            nodes=self.nodes,
            rpc_node=self.rpc_node,
            account_prefixes=ALL_ACCOUNTS_PREFIXES,
            epoch_length=EPOCH_LENGTH)

        self.testcase.wait_for_blocks(3)

        self.testcase.create_accounts()

        self.testcase.deploy_contracts()

        target_height = self.__next_target_height(num_epochs=1)
        logger.info(
            f"Step 1: Running with memtries enabled until height {target_height}"
        )
        self.testcase.random_workload_until(target_height)

        target_height = self.__next_target_height(num_epochs=1)
        logger.info(
            f"Step 2: Restarting nodes with memtries disabled until height {target_height}"
        )
        self.__restart_nodes(enable_memtries=False)
        self.testcase.random_workload_until(target_height)

        target_height = self.__next_target_height(num_epochs=1)
        logger.info(
            f"Step 3: Restarting nodes with memtries enabled until height {target_height}"
        )
        self.__restart_nodes(enable_memtries=True)
        self.testcase.random_workload_until(target_height)

        self.testcase.wait_for_txs(assert_all_accepted=False)
        logger.info("Test ended")

    def __next_target_height(self, num_epochs):
        """Returns a next target height until which we will send the transactions."""
        current_height = self.testcase.wait_for_blocks(1)
        stop_height = random.randint(1, EPOCH_LENGTH)
        return current_height + num_epochs * EPOCH_LENGTH + stop_height

    def __restart_nodes(self, enable_memtries):
        """Stops and restarts the nodes with the config that enables/disables memtries.
        
        It restarts only the validator nodes and does NOT restart the RPC node."""
        boot_node = self.rpc_node
        for i in range(0, NUM_VALIDATORS):
            self.nodes[i].kill()
            time.sleep(2)
            self.nodes[i].change_config(
                {"store.load_mem_tries_for_tracked_shards": enable_memtries})
            self.nodes[i].start(boot_node=None if i == 0 else boot_node)


if __name__ == '__main__':
    unittest.main()
