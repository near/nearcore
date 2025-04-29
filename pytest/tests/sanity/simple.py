#!/usr/bin/env python3
# A simple test that is a good template for writing new tests.
# Spins up a few nodes and waits for a few blocks to be produced.
# Usage:
# python3 pytest/tests/sanity/simple.py
# python3 pytest/tests/sanity/simple.py SimpleTest.test_simple

import unittest
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import init_cluster, load_config, spin_up_node, start_cluster
from utils import wait_for_blocks


class SimpleTest(unittest.TestCase):

    # Spin up a single node and wait for a few blocks.
    # Please note that this uses the default neard config and is pretty slow.
    def test_simple(self):
        logger.info("The simple test is starting.")

        [node] = start_cluster(
            num_nodes=1,
            num_observers=0,
            num_shards=1,
            config=None,
            genesis_config_changes=[],
            client_config_changes={},
            message_handler=None,
        )

        wait_for_blocks(node, target=20)

        # It's important to kill the nodes at the end of the test when there are
        # multiple tests in the test suite. Otherwise the nodes from different
        # tests can be mixed up.
        node.kill()

        logger.info("The simple test is finished.")

    # Spin up a single node and wait for a few blocks. This test case is
    # customized to showcase the various configuration options.
    def test_custom(self):
        logger.info("The custom test is starting.")

        test_config = load_config()
        epoch_length = 10
        genesis_config_changes = [
            ("epoch_length", epoch_length),
        ]
        client_config_changes = {
            0: {
                'archive': True,
                'tracked_shards_config': 'AllShards',
            },
        }

        near_root, [node_dir] = init_cluster(
            num_nodes=1,
            num_observers=0,
            num_shards=1,
            config=test_config,
            genesis_config_changes=genesis_config_changes,
            client_config_changes=client_config_changes,
            prefix="test_custom_",
        )

        node = spin_up_node(
            test_config,
            near_root,
            node_dir,
            0,
        )

        wait_for_blocks(node, target=2 * epoch_length)

        # It's important to kill the nodes at the end of the test when there are
        # multiple tests in the test suite. Otherwise the nodes from different
        # tests can be mixed up.
        node.kill()

        logger.info("The custom test is finished.")


if __name__ == '__main__':
    unittest.main()
