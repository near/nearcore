#!/usr/bin/env python3

# Test for checking error handling during resharding. Spins up a few nodes from
# genesis with the previous shard layout. Stops the nodes in the middle of the
# epoch before resharding and corrupts the state snapshot. Resumes the nodes and
# verifies that the error is reported correctly.

# Usage:
# python3 pytest/tests/sanity/resharding_error_handling.py

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import corrupt_state_snapshot, get_binary_protocol_version, init_cluster, load_config, spin_up_node
from utils import MetricsTracker, poll_blocks, wait_for_blocks
from resharding_lib import get_genesis_shard_layout_version, get_target_shard_layout_version, get_genesis_num_shards, get_target_num_shards, get_genesis_config_changes, get_client_config_changes


class ReshardingErrorHandlingTest(unittest.TestCase):

    def setUp(self) -> None:
        self.epoch_length = 10
        self.config = load_config()
        self.binary_protocol_version = get_binary_protocol_version(self.config)
        assert self.binary_protocol_version is not None

        self.genesis_shard_layout_version = get_genesis_shard_layout_version(
            self.binary_protocol_version)
        self.target_shard_layout_version = get_target_shard_layout_version(
            self.binary_protocol_version)

        self.genesis_num_shards = get_genesis_num_shards(
            self.binary_protocol_version)
        self.target_num_shards = get_target_num_shards(
            self.binary_protocol_version)

    # timeline by block number
    # epoch_length + 2 - snapshot is requested
    # epoch_length + 3 - snapshot is finished
    # epoch_length + 4 - stop the nodes, corrupt the snapshot, start nodes
    # epoch_length + 4 - resharding starts and fails
    # epoch_length * 2 + 1 - last block while node is still healthy before chain
    # upgrades to the new shard layout
    def test_resharding(self):
        logger.info("The resharding test is starting.")
        num_nodes = 2

        genesis_config_changes = get_genesis_config_changes(
            self.epoch_length, self.binary_protocol_version, logger)
        client_config_changes = get_client_config_changes(num_nodes, 10)

        near_root, [node0_dir, node1_dir] = init_cluster(
            num_nodes=num_nodes,
            num_observers=0,
            num_shards=1,
            config=self.config,
            genesis_config_changes=genesis_config_changes,
            client_config_changes=client_config_changes,
        )

        node0 = spin_up_node(
            self.config,
            near_root,
            node0_dir,
            0,
        )
        node1 = spin_up_node(
            self.config,
            near_root,
            node1_dir,
            1,
            boot_node=node0,
        )

        logger.info("wait until the snapshot is ready")
        wait_for_blocks(node0, target=self.epoch_length + 4)
        wait_for_blocks(node1, target=self.epoch_length + 4)

        logger.info("the snapshot should be ready, stopping nodes")
        node0.kill(gentle=True)
        node1.kill(gentle=True)

        logger.info("corrupting the state snapshot of node0")
        output = corrupt_state_snapshot(
            self.config,
            node0_dir,
            self.genesis_shard_layout_version,
        )
        logger.info(f"corrupted state snapshot\n{output}")

        # Update the initial delay to start resharding as soon as possible.
        client_config_changes = get_client_config_changes(1, 0)[0]
        node0.change_config(client_config_changes)
        node1.change_config(client_config_changes)

        logger.info("restarting nodes")
        node0.start()
        node1.start(boot_node=node0)

        all_failed_observed = False

        metrics = MetricsTracker(node0)
        for height, _ in poll_blocks(node0):
            status = metrics.get_metric_all_values("near_resharding_status")
            logger.info(f"#{height} resharding status {status}")

            if len(status) > 0:
                all_failed = all([s == -1.0 for (_, s) in status])
                all_failed_observed = all_failed_observed or all_failed

            # The node should be able to survive until the end of the epoch even
            # though resharding is broken. Only break after the last block of epoch.
            if height >= self.epoch_length * 2:
                break

        node0.kill(gentle=True)
        node1.kill(gentle=True)

        # Resharding should fail for all shards.
        self.assertTrue(all_failed_observed)

        logger.info("The resharding error handling test is finished.")


if __name__ == '__main__':
    unittest.main()
