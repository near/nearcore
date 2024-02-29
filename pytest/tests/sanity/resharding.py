#!/usr/bin/env python3

# Small test for resharding. Spins up a few nodes from genesis with the previous
# shard layout, waits for a few epochs and verifies that the shard layout is
# upgraded.
# Usage:
# python3 pytest/tests/sanity/resharding.py
# RUST_LOG=info,resharding=debug,sync=debug,catchup=debug python3 pytest/tests/sanity/resharding.py

import pathlib
import sys

import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import init_cluster, spin_up_node
from utils import MetricsTracker, poll_blocks
from resharding_lib import ReshardingTestBase, get_genesis_config_changes, get_client_config_changes


class ReshardingTest(ReshardingTestBase):

    def setUp(self) -> None:
        super().setUp(epoch_length=5)

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

        for height, hash in poll_blocks(node0):
            version = self.get_version(metrics_tracker)
            num_shards = self.get_num_shards(metrics_tracker)

            protocol_config = node0.json_rpc(
                "EXPERIMENTAL_protocol_config",
                {"block_id": hash},
            )

            self.assertTrue('error' not in protocol_config)

            self.assertTrue('result' in protocol_config)
            protocol_config = protocol_config.get('result')

            self.assertTrue('shard_layout' in protocol_config)
            shard_layout = protocol_config.get('shard_layout')

            self.assertTrue('V1' in shard_layout)
            shard_layout = shard_layout.get('V1')

            self.assertTrue('boundary_accounts' in shard_layout)
            boundary_accounts = shard_layout.get('boundary_accounts')

            logger.info(
                f"#{height} shard layout version: {version}, num shards: {num_shards}"
            )
            logger.debug(f"#{height} shard layout: {shard_layout}")

            # check the shard layout versions from metrics and from json rpc are equal
            self.assertEqual(version, shard_layout.get('version'))
            # check the shard num from metrics and json rpc are equal
            self.assertEqual(num_shards, len(boundary_accounts) + 1)

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
