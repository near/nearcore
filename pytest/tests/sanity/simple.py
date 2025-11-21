#!/usr/bin/env python3
# A simple test that is a good template for writing new tests.
# Spins up a few nodes and waits for a few blocks to be produced.
# Usage:
# python3 pytest/tests/sanity/simple.py
# python3 pytest/tests/sanity/simple.py SimpleTest.test_simple

import json
import os
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

        rpc_polling_config = {
            "rpc": {
                "polling_config": {
                    "polling_timeout": {"secs": 20, "nanos": 0},
                    "polling_interval": {"secs": 0, "nanos": 10000000},
                }
            }
        }

        client_config_changes = {
            0: rpc_polling_config,
            1: rpc_polling_config,
        }

        num_validators = 2
        num_mpc_nodes = 1
        nodes = start_cluster(
            num_validators,
            num_mpc_nodes,
            1,
            None,
            [("epoch_length", 1000), ("block_producer_kickout_threshold", 80)],
            client_config_changes=client_config_changes
        )

        validators = nodes[:num_validators]
        observers = nodes[num_validators:]

        wait_for_blocks(validators[0], target=20)

        logger.info("The simple test is finished.")


if __name__ == '__main__':
    unittest.main()
