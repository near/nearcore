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

from time import sleep
import unittest
import sys
import pathlib
import base58

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import BaseNode, init_cluster, spin_up_node
from utils import MetricsTracker, poll_blocks
from resharding_lib import ReshardingTestBase, get_genesis_config_changes, get_client_config_changes
from transaction import sign_payment_tx


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

        for height, hash in poll_blocks(node0):
            version = self.get_version(metrics_tracker)
            num_shards = self.get_num_shards(metrics_tracker)
            resharding_status = self.get_resharding_status(metrics_tracker)
            flat_storage_head = self.get_flat_storage_head(metrics_tracker)

            logger.info(
                f"#{height} shard layout version: {version}, num shards: {num_shards}"
            )
            logger.info(
                f"${height} resharding status: {self.__pretty_metric_list(resharding_status)}"
            )
            logger.info(
                f"#{height} flat storage head: {self.__pretty_metric_list(flat_storage_head)}"
            )

            self.assertEqual(version, self.genesis_shard_layout_version)
            self.assertEqual(num_shards, self.genesis_num_shards)

            # - before resharding is finished flat storage should only exist for
            #   the old shards
            # - after resharding is finished flat storage should exists for both
            #   old and new shards.
            # Asserting that condition exactly is prone to race conditions. Here
            # we only check it briefly but after the restart we check it properly.
            self.assertGreaterEqual(len(flat_storage_head), num_shards)

            self.assertLess(height, 4 * self.epoch_length)

            # Resharding must finish before the end of the epoch, otherwise this
            # test wouldn't check anything.
            self.assertLess(height, target_height)

            # Send some transactions so that
            # a) the state gets changed
            # b) the flat storage gets updated
            self.__send_tx(node0, hash, height)

            if self.__is_resharding_finished(resharding_status):
                break

        # Wait a little bit to make sure that resharding is correctly
        # postprocessed.
        sleep(1)

        # Resharding is finished, restart the node now and check that resharding
        # was resumed and not restarted.
        logger.info("resharding finished")

        node0.kill()
        node0.start()

        check_count = 0
        for height, hash in poll_blocks(node0):
            if height > 4 * self.epoch_length:
                break

            version = self.get_version(metrics_tracker)
            num_shards = self.get_num_shards(metrics_tracker)
            resharding_status = self.get_resharding_status(metrics_tracker)

            flat_storage_head = self.get_flat_storage_head(metrics_tracker)

            # The node needs a short while to set the metrics.
            if version is None or num_shards is None or resharding_status is None:
                continue

            logger.info(
                f"#{height} shard layout version: {version}, num shards: {num_shards}"
            )
            logger.info(
                f"${height} resharding status: {self.__pretty_metric_list(resharding_status)}"
            )
            logger.info(
                f"#{height} flat storage head: {self.__pretty_metric_list(flat_storage_head)}"
            )

            # GOOD If resharding is correctly resumed the status should remain empty.
            # BAD  If resharding is restarted the status would be non-empty.
            # BAD  If resharding is neither restarted nor resumed the node would get
            # stuck at target_height - 1. This is checked below.
            self.assertEqual(resharding_status, [])

            # After the restart, which happens after the resharding is
            # finished, the flat storage should exist for both old and new
            # shards. Technically once we move to the new epoch the flat storage
            # for old shards is not needed anymore but we don't exactly clean up
            # the metrics so it kinda hangs around until the end of the test.
            expected_len = self.genesis_num_shards + self.target_num_shards
            self.assertEqual(len(flat_storage_head), expected_len)

            if height <= target_height:
                self.assertEqual(version, self.genesis_shard_layout_version)
                self.assertEqual(num_shards, self.genesis_num_shards)

            else:
                self.assertEqual(version, self.target_shard_layout_version)
                self.assertEqual(num_shards, self.target_num_shards)

            check_count += 1

            # Send some transactions so that
            # a) the state gets changed
            # b) the flat storage gets updated
            self.__send_tx(node0, hash, height)

        # Make sure that we actually checked something after the restart.
        self.assertGreater(check_count, 0)

        node0.kill()
        node1.kill()

        logger.info("The resharding restart test is finished.")

    def __pretty_metric_list(self, metric_list):
        result = dict()
        for metric in metric_list:
            (shard_uid, value) = metric
            shard_uid = shard_uid.get('shard_uid')
            result[shard_uid] = value

        return result

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

    def __send_tx(self, node: BaseNode, block_hash: str, nonce: int):
        tx = sign_payment_tx(
            node.signer_key,
            'test1',
            1,
            nonce,
            base58.b58decode(block_hash.encode('utf8')),
        )
        result = node.send_tx(tx)
        self.assertTrue('result' in result)
        self.assertTrue('error' not in result)


if __name__ == '__main__':
    unittest.main()
