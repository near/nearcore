#!/usr/bin/python3
"""
 Spins up an archival node with cold store configured and verifies that blocks
 are copied from hot to cold store.
"""

import sys
import pathlib
import os
import copy
import json
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from utils import wait_for_blocks
from cluster import init_cluster, spin_up_node, load_config, get_config_json, set_config_json
from configured_logger import logger


class TestRpcFinality(unittest.TestCase):

    def test_base_case(self):
        config = load_config()
        near_root, [node_dir] = init_cluster(1, 0, 1, config, [], {})

        node_config = get_config_json(node_dir)

        node_config["archive"] = True
        # Need to create a deepcopy of the store config, otherwise
        # store and cold store will point to the same instance and
        # both will end up with the same path.
        node_config["cold_store"] = copy.deepcopy(node_config["store"])
        node_config["store"]["path"] = os.path.join(node_dir, 'data')
        node_config["cold_store"]["path"] = os.path.join(node_dir, 'cold_data')

        set_config_json(node_dir, node_config)

        node = spin_up_node(config, near_root, node_dir, 0, single_node=True)

        # Wait until 20 blocks are produced so that we're guaranteed that
        # cold head has enough time to move. cold_head <= final_head < head
        n = 20
        wait_for_blocks(node, target=n)

        split_storage_info = node.json_rpc("EXPERIMENTAL_split_storage_info",
                                           {})

        logger.info(
            f"split storage info \n{json.dumps(split_storage_info, indent=2)}")

        self.assertNotIn("error", split_storage_info)
        self.assertIn("result", split_storage_info)
        result = split_storage_info["result"]
        head_height = result["head_height"]
        final_head_height = result["final_head_height"]
        cold_head_height = result["cold_head_height"]

        self.assertGreaterEqual(head_height, n)
        self.assertGreaterEqual(final_head_height, n - 3)
        self.assertGreaterEqual(cold_head_height, final_head_height - 3)

        node.kill(gentle=True)


if __name__ == "__main__":
    unittest.main()
