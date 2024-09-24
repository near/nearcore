#!/usr/bin/env python3
# Spins up 4 validating nodes. Let validators track a single shard.
# Add a dumper node for the state sync headers.
# Add an RPC node to issue tx and change the state.
# Send random transactions between accounts in different shards.
# Shuffle the shard assignment of validators and check if they can sync up.

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import start_cluster
import state_sync_lib
from utils import wait_for_blocks
import simple_test

EPOCH_LENGTH = 10

NUM_VALIDATORS = 4

# Shard layout with 5 roughly equal size shards for convenience.
SHARD_LAYOUT = {
    "V1": {
        "boundary_accounts": [
            "fff",
            "lll",
            "rrr",
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
    "sss",
]


class StateSyncValidatorShardSwap(unittest.TestCase):

    def _prepare_cluster(self, with_rpc=False, shuffle_shard_assignment=False):
        (node_config_dump,
         node_config_sync) = state_sync_lib.get_state_sync_configs_pair(
             tracked_shards=None)

        # State snapshot is disabled for dumper. We only want to dump the headers.
        node_config_dump["store.state_snapshot_enabled"] = False
        node_config_dump[
            "store.state_snapshot_config.state_snapshot_type"] = "ForReshardingOnly"

        # State snapshot is enabled for validators. They will share parts of the state.
        node_config_sync["store.state_snapshot_enabled"] = True
        node_config_sync["tracked_shards"] = []

        # Validators
        configs = {x: node_config_sync.copy() for x in range(NUM_VALIDATORS)}

        # Dumper
        configs[NUM_VALIDATORS] = node_config_dump

        if with_rpc:
            # RPC
            configs[NUM_VALIDATORS + 1] = node_config_sync.copy()
            # RPC tracks all shards.
            configs[NUM_VALIDATORS + 1]["tracked_shards"] = [0]
            # RPC node does not participate in state parts distribution.
            configs[NUM_VALIDATORS + 1]["store.state_snapshot_enabled"] = False
            configs[NUM_VALIDATORS + 1][
                "store.state_snapshot_config.state_snapshot_type"] = "ForReshardingOnly"

        nodes = start_cluster(
            num_nodes=NUM_VALIDATORS,
            num_observers=1 + (1 if with_rpc else 0),
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[
                ["epoch_length", EPOCH_LENGTH], ["shard_layout", SHARD_LAYOUT],
                [
                    "shuffle_shard_assignment_for_chunk_producers",
                    shuffle_shard_assignment
                ], ["block_producer_kickout_threshold", 0],
                ["chunk_producer_kickout_threshold", 0]
            ],
            client_config_changes=configs)

        for node in nodes:
            node.stop_checking_store()

        self.dumper_node = nodes[NUM_VALIDATORS]
        self.rpc_node = nodes[NUM_VALIDATORS +
                              1] if with_rpc else self.dumper_node
        self.nodes = nodes
        self.validators = nodes[:NUM_VALIDATORS]

    def _prepare_simple_transfers(self):
        self.testcase = simple_test.SimpleTransferBetweenAccounts(
            nodes=self.nodes,
            rpc_node=self.rpc_node,
            account_prefixes=ALL_ACCOUNTS_PREFIXES,
            epoch_length=EPOCH_LENGTH)

        self.testcase.wait_for_blocks(3)

        self.testcase.create_accounts()

        self.testcase.deploy_contracts()

    def _clear_cluster(self):
        self.testcase = None
        for node in self.nodes:
            node.cleanup()

    def test_state_sync_with_shard_swap(self):
        # Dumper node will not track any shard. So we need a dedicated RPC node.
        # TODO: enable shuffle_shard_assignment after decentralized state sync is implemented.
        self._prepare_cluster(with_rpc=True, shuffle_shard_assignment=False)
        self._prepare_simple_transfers()

        target_height = 6 * EPOCH_LENGTH
        self.testcase.random_workload_until(target_height)

        # Wait for all nodes to reach epoch 6.
        for n in self.validators:
            wait_for_blocks(n, target=target_height)
        logger.info("Test ended")

    def tearDown(self):
        self._clear_cluster()


if __name__ == '__main__':
    unittest.main()
