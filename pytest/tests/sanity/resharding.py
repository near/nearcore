#!/usr/bin/env python3

# Small test for resharding. Spins up a few nodes from genesis with the previous
# shard layout, waits for a few epochs and verifies that the shard layout is
# upgraded.
# Usage:
# python3 pytest/tests/sanity/resharding.py

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import get_binary_protocol_version, init_cluster, load_config, spin_up_node
from utils import MetricsTracker, poll_blocks
from resharding_lib import get_genesis_shard_layout_version, get_target_shard_layout_version, get_genesis_num_shards, get_target_num_shards, get_epoch_offset, get_genesis_config_changes, get_client_config_changes


class ReshardingTest(unittest.TestCase):

    def setUp(self) -> None:
        self.epoch_length = 5
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

        self.epoch_offset = get_epoch_offset(self.binary_protocol_version)

    def test_resharding(self):
        logger.info("The resharding test is starting.")
        num_nodes = 2

        genesis_config_changes = get_genesis_config_changes(
            self.epoch_length, self.binary_protocol_version, logger)
        client_config_changes = get_client_config_changes(num_nodes)

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

        metrics_tracker = MetricsTracker(node0)

        for height, _ in poll_blocks(node0):
            version = metrics_tracker.get_int_metric_value(
                "near_shard_layout_version")
            num_shards = metrics_tracker.get_int_metric_value(
                "near_shard_layout_num_shards")

            logger.info(
                f"#{height} shard layout version: {version}, num shards: {num_shards} "
            )

            # This may be flaky - it shouldn't - but it may. We collect metrics
            # after the block is processed. If there is some delay the shard
            # layout may change and the assertions below will fail.

            # TODO(resharding) Why is epoch offset needed here?
            if height <= 2 * self.epoch_length + self.epoch_offset:
                self.assertEqual(version, self.genesis_shard_layout_version)
                self.assertEqual(num_shards, self.genesis_num_shards)
            else:
                self.assertEqual(version, self.target_shard_layout_version)
                self.assertEqual(num_shards, self.target_num_shards)

            if height >= 4 * self.epoch_length:
                break

        node0.kill()
        node1.kill()

        logger.info("The resharding test is finished.")


if __name__ == '__main__':
    unittest.main()
