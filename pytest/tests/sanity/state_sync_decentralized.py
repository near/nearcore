#!/usr/bin/env python3
# Spins up 4 validating nodes. Let validators track a single shard.
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
from utils import wait_for_blocks, MetricsTracker
import simple_test

EPOCH_LENGTH = 10

NUM_VALIDATORS = 4

# Shard layout with 4 roughly equal size shards for convenience.
SHARD_LAYOUT = {
    "V1": {
        "boundary_accounts": [
            "fff",
            "lll",
            "rrr",
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
    "sss",
]


class StateSyncValidatorShardSwap(unittest.TestCase):

    def _prepare_cluster(self):
        node_config = state_sync_lib.get_state_sync_config_p2p(
            tracked_shards_config="NoShards")

        # Validators
        configs = {x: node_config.copy() for x in range(NUM_VALIDATORS)}

        # RPC
        configs[NUM_VALIDATORS] = node_config.copy()
        # RPC tracks all shards.
        configs[NUM_VALIDATORS]["tracked_shards_config"] = "AllShards"
        # RPC node does not serve state sync snapshots.
        configs[NUM_VALIDATORS][
            "store.state_snapshot_config.state_snapshot_type"] = "Disabled"

        nodes = start_cluster(
            num_nodes=NUM_VALIDATORS,
            num_observers=1,
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[
                # Rotate tracked shards to trigger state sync
                ["shuffle_shard_assignment_for_chunk_producers", True],
                ["epoch_length", EPOCH_LENGTH],
                ["shard_layout", SHARD_LAYOUT],
                ["block_producer_kickout_threshold", 0],
                ["chunk_producer_kickout_threshold", 0]
            ],
            client_config_changes=configs)

        for node in nodes:
            node.stop_checking_store()

        self.nodes = nodes
        self.validators = nodes[:NUM_VALIDATORS]
        self.rpc_node = nodes[NUM_VALIDATORS]

    def _prepare_simple_transfers(self):
        self.testcase = simple_test.SimpleTransferBetweenAccounts(
            nodes=self.nodes,
            rpc_node=self.rpc_node,
            account_prefixes=ALL_ACCOUNTS_PREFIXES,
            epoch_length=EPOCH_LENGTH)

        self.testcase.wait_for_blocks(3, timeout=10)

        self.testcase.create_accounts()

        self.testcase.deploy_contracts()

    def _clear_cluster(self):
        self.testcase = None
        for node in self.nodes:
            node.cleanup()

    def test_state_sync_with_shard_swap(self):
        self._prepare_cluster()
        self._prepare_simple_transfers()

        target_height = 6 * EPOCH_LENGTH
        self.testcase.random_workload_until(target_height)

        # Wait for all nodes to reach epoch 6.
        for n in self.validators:
            wait_for_blocks(n, target=target_height, timeout=120)
        logger.info("Test ended")

        for i in range(len(self.nodes) - 1):
            metrics = MetricsTracker(self.nodes[i])
            down = metrics.get_metric_all_values(
                "near_state_sync_download_result")

            num_headers = 0
            num_parts = 0
            num_retries = 0

            for key, value in down:
                if key['result'] != 'success':
                    num_retries += 1
                    continue

                if key['source'] != 'network':
                    assert False, f"Expected success only from 'network', got {key['source']}"

                if key['type'] == 'header':
                    num_headers += 1
                elif key['type'] == 'part':
                    num_parts += 1
                else:
                    assert False, f"Unexpected near_state_sync_download_result value ({key}, {value})"

            print(
                f"Node {i} downloaded {num_headers} state headers and {num_parts} parts from peers"
            )
            if num_retries > 0:
                print(
                    f"WARN: Node {i} made {num_retries} unsuccessful requests for state data"
                )
            assert num_headers > 0 and num_parts > 0, f"Node {i} did not state sync, but is expected to in this test"

    def tearDown(self):
        self._clear_cluster()


if __name__ == '__main__':
    unittest.main()
