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


class TestSplitStorage(unittest.TestCase):

    def _pretty_json(self, value):
        return json.dumps(value, indent=2)

    def _get_split_storage_info(self, node):
        return node.json_rpc("EXPERIMENTAL_split_storage_info", {})

    def _configure_archive(self, node_dir):
        node_config = get_config_json(node_dir)
        node_config["archive"] = True
        set_config_json(node_dir, node_config)

    def _configure_cold_storage(self, node_dir):
        node_config = get_config_json(node_dir)

        # Need to create a deepcopy of the store config, otherwise
        # store and cold store will point to the same instance and
        # both will end up with the same path.
        node_config["cold_store"] = copy.deepcopy(node_config["store"])
        node_config["store"]["path"] = os.path.join(node_dir, 'data')
        node_config["cold_store"]["path"] = os.path.join(node_dir, 'cold_data')

        set_config_json(node_dir, node_config)

    # Configure cold storage and start neard. Wait for a few blocks
    # and verify that cold head is moving and that it's close behind
    # final head.
    def test_base_case(self):
        print()
        logger.info(f"starting test_base_case")

        config = load_config()
        near_root, [node_dir] = init_cluster(1, 0, 1, config, [], {},
                                             "test_base_case_")

        self._configure_archive(node_dir)
        self._configure_cold_storage(node_dir)

        node = spin_up_node(config, near_root, node_dir, 0, single_node=True)

        # Wait until 20 blocks are produced so that we're guaranteed that
        # cold head has enough time to move. cold_head <= final_head < head
        n = 20
        wait_for_blocks(node, target=n)

        split_storage_info = self._get_split_storage_info(node)
        pretty_split_storage_info = self._pretty_json(split_storage_info)
        logger.info(f"split storage info \n{pretty_split_storage_info}")

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

    # Do not configure cold storage and start neard. Wait for a few blocks,
    # stop neard and only then configure cold store. Wait for a short while
    # again and check that the cold head has caught up.
    def test_migration(self):
        print()
        logger.info(f"starting test_migration")

        config = load_config()
        near_root, [node_dir] = init_cluster(1, 0, 1, config, [], {},
                                             "test_migration_")

        self._configure_archive(node_dir)

        node = spin_up_node(config, near_root, node_dir, 0, single_node=True)

        # Wait until 10 blocks are produced so that we're sure that the db is
        # properly created and populated with some data.
        n = 10
        n = wait_for_blocks(node, target=n).height

        split_storage_info = self._get_split_storage_info(node)
        pretty_split_storage_info = self._pretty_json(split_storage_info)
        logger.info(f"split storage info before \n{pretty_split_storage_info}")

        node.kill(gentle=True)

        self.assertNotIn("error", split_storage_info)
        self.assertIn("result", split_storage_info)
        result = split_storage_info["result"]
        head_height = result["head_height"]
        final_head_height = result["final_head_height"]
        cold_head_height = result["cold_head_height"]
        hot_db_kind = result["hot_db_kind"]

        self.assertGreaterEqual(head_height, n)
        self.assertGreaterEqual(final_head_height, n - 3)
        # The cold storage is not configured so cold head should be none.
        self.assertIsNone(cold_head_height)
        # The hot db kind should remain archive until fully migrated.
        self.assertEqual(hot_db_kind, "Archive")

        self._configure_cold_storage(node_dir)

        node.start()

        # Wait until 20 blocks are produced so that cold store loop has enough
        # time to catch up.
        n += 20
        wait_for_blocks(node, target=n)

        split_storage_info = self._get_split_storage_info(node)
        pretty_split_storage_info = self._pretty_json(split_storage_info)
        logger.info(f"split storage info after \n{pretty_split_storage_info}")

        self.assertNotIn("error", split_storage_info)
        self.assertIn("result", split_storage_info)
        result = split_storage_info["result"]
        head_height = result["head_height"]
        final_head_height = result["final_head_height"]
        cold_head_height = result["cold_head_height"]
        hot_db_kind = result["hot_db_kind"]

        self.assertGreaterEqual(head_height, n)
        self.assertGreaterEqual(final_head_height, n - 3)
        # The cold storage head should be fully caught up by now.
        self.assertGreaterEqual(cold_head_height, final_head_height - 3)
        # The hot db kind should remain archive until fully migrated.
        self.assertEqual(hot_db_kind, "Archive")

        node.kill(gentle=True)


if __name__ == "__main__":
    unittest.main()
