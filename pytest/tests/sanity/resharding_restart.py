#!/usr/bin/env python3
"""
Testing that resharding is correctly resumed after a node restart. The goal is
to make sure that if the node is restarted after resharding is finished that it
picks up where it left and doesn't try to start again.

Note that this is different than what happens if node is restared in the middle
of resharding. In this case resharding would get restarted from the beginning.

Usage:

python3 pytest/tests/sanity/resharding_restart.py

"""

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import init_cluster, spin_up_node
from utils import MetricsTracker, poll_blocks
from resharding_lib import ReshardingTestBase, get_genesis_config_changes, get_client_config_changes


class ReshardingTest(ReshardingTestBase):

    def setUp(self) -> None:
        # Note - this test has a really long epoch because it takes the nodes
        # forever to take the flat state snapshot. Ideally this should be
        # fixed and the test shortened.
        super().setUp(epoch_length=20)

    def test_resharding_restart(self):
        logger.info("The resharding restart test is starting.")
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

        # The target height is the height where the new shard layout should
        # first be used.
        target_height = 2 * self.epoch_length + self.epoch_offset
        metrics_tracker = MetricsTracker(node0)

        for height, _ in poll_blocks(node0):
            version = self.get_version(metrics_tracker)
            num_shards = self.get_num_shards(metrics_tracker)
            resharding_status = self.get_resharding_status(metrics_tracker)

            logger.info(
                f"#{height} shard layout version: {version}, num shards: {num_shards}, status: {resharding_status}"
            )

            self.assertEqual(version, self.genesis_shard_layout_version)
            self.assertEqual(num_shards, self.genesis_num_shards)

            self.assertLess(height, 4 * self.epoch_length)

            # Resharding must finish before the end of the epoch, otherwise this
            # test wouldn't check anything.
            self.assertLess(height, target_height)

            if self.__is_resharding_finished(resharding_status):
                break

        # Resharding is finished, restart the node now and check that resharding
        # was resumed and not restarted.
        logger.info("resharding finished")

        node0.kill()
        node0.start()

        check_count = 0
        for height, _ in poll_blocks(node0):
            if height > 4 * self.epoch_length:
                break

            version = self.get_version(metrics_tracker)
            num_shards = self.get_num_shards(metrics_tracker)
            resharding_status = self.get_resharding_status(metrics_tracker)

            # The node needs a short while to set the metrics.
            if version is None or num_shards is None or resharding_status is None:
                continue

            logger.info(
                f"#{height} shard layout version: {version}, num shards: {num_shards}, status: {resharding_status}"
            )

            # If resharding is correctly resumed the status should remain empty.
            # If resharding is restarted the status would be non-empty.
            # If resharding is neither restarted nor resumed the node would get
            # stuck at target_height - 1.
            self.assertEqual(resharding_status, [])

            if height <= target_height:
                self.assertEqual(version, self.genesis_shard_layout_version)
                self.assertEqual(num_shards, self.genesis_num_shards)
            else:
                self.assertEqual(version, self.target_shard_layout_version)
                self.assertEqual(num_shards, self.target_num_shards)

            check_count += 1

        # Make sure that we actually checked something after the restart.
        self.assertGreater(check_count, 0)

        node0.kill()
        node1.kill()

        logger.info("The resharding restart test is finished.")

    def __is_resharding_finished(self, all_shards_status):
        if len(all_shards_status) == 0:
            logger.debug("is resharding finished: no shards")
            return False

        all_finished = True
        for shard_status in all_shards_status:
            (shard_uid, status) = shard_status
            if int(status) == 2:
                logger.debug(f"is resharding finished: {shard_uid} ready")
            else:
                logger.debug(f"is resharding finished: {shard_uid} not ready")
                all_finished = False

        return all_finished


if __name__ == '__main__':
    unittest.main()
