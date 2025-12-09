#!/usr/bin/python3
"""
 Spins up an archival node with cold store configured and verifies that blocks
 are copied from hot to cold store.
"""

import json
import pathlib
import sys
import time
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import (get_config_json, init_cluster, load_config,
                     set_config_json, spin_up_node)
from configured_logger import logger

from utils import wait_for_blocks


class TestSplitStorage(unittest.TestCase):

    def _steps(self):
        for name in dir(self):  # dir() result is implicitly sorted
            if name.startswith("step"):
                yield name, getattr(self, name)

    def test_steps(self):
        for _, step in self._steps():
            step()
            time.sleep(5)

    def _pretty_json(self, value):
        return json.dumps(value, indent=2)

    def _get_split_storage_info(self, node):
        return node.json_rpc("EXPERIMENTAL_split_storage_info", {})

    def _configure_hot_storage(self, node_dir, new_path):
        node_config = get_config_json(node_dir)
        node_config["store"]["path"] = new_path
        set_config_json(node_dir, node_config)

    def _check_split_storage_info(
        self,
        msg,
        node,
        expected_head_height,
        check_cold_head,
    ):
        info = self._get_split_storage_info(node)
        pretty_info = self._pretty_json(info)
        logger.info(f"Checking split storage info for the {msg}")
        logger.info(f"The split storage info is \n{pretty_info}")

        self.assertNotIn("error", info)
        self.assertIn("result", info)
        result = info["result"]
        head_height = result["head_height"]
        final_head_height = result["final_head_height"]
        cold_head_height = result["cold_head_height"]
        hot_db_kind = result["hot_db_kind"]

        self.assertEqual(hot_db_kind, "Hot")
        self.assertGreaterEqual(head_height, expected_head_height)
        self.assertGreaterEqual(final_head_height, expected_head_height - 10)
        if check_cold_head:
            self.assertGreaterEqual(cold_head_height, final_head_height - 10)
        else:
            self.assertIsNone(cold_head_height)

    # Configure cold storage and start neard. Wait for a few blocks
    # and verify that cold head is moving and that it's close behind
    # final head.
    def step1_base_case_test(self):
        logger.info(f"starting test_base_case")

        config = load_config()
        client_config_changes = {
            0: {
                'archive': True,
                'tracked_shards_config': 'AllShards',
                'save_trie_changes': True,
            },
        }

        epoch_length = 5
        genesis_config_changes = [
            ("epoch_length", epoch_length),
        ]

        near_root, [node_dir] = init_cluster(
            1,
            0,
            1,
            config,
            genesis_config_changes,
            client_config_changes,
            "test_base_case_",
        )

        node = spin_up_node(config, near_root, node_dir, 0, single_node=True)

        # Wait until enough blocks are produced so that we're guaranteed that
        # cold head has enough time to move.
        # TODO consider reducing n to speed up the test.
        n = 50
        wait_for_blocks(node, target=n)

        self._check_split_storage_info(
            "base_case",
            node=node,
            expected_head_height=n,
            check_cold_head=True,
        )

        node.kill()
        logger.info(f"Stopped node0 from base_case_test")

    # Spin up two archival split storage nodes.
    # Pause the second one.
    # After hot tail of the first one than head of the second one, restart the second one.
    # Make sure that the second one is able to sync after that.
    def step2_archival_node_sync_test(self):
        logger.info(f"Starting the archival <- split storage sync test")

        # Archival nodes do not run state sync. This means that if peers
        # ran away further than epoch_length * gc_epoch_num, archival nodes
        # will not be able to further sync. In practice it means we need a long
        # enough epoch_length or more gc_epoch_num to keep.
        epoch_length = 10
        gc_epoch_num = 3

        genesis_config_changes = [
            ("epoch_length", epoch_length),
        ]
        client_config_changes = {
            0: {
                'archive': False,
                'tracked_shards_config': 'AllShards',
                'state_sync_enabled': True
            },
            1: {
                'archive': True,
                'tracked_shards_config': 'AllShards',
                'save_trie_changes': True,
                'split_storage': {
                    'enable_split_storage_view_client': True
                },
            },
            2: {
                'archive': True,
                'tracked_shards_config': 'AllShards',
                'split_storage': {
                    'enable_split_storage_view_client': True
                },
            },
            3: {
                'archive': False,
                'tracked_shards_config': 'AllShards',
                'gc_num_epochs_to_keep': gc_epoch_num,
                'state_sync_enabled': True
            },
        }
        config = load_config()
        near_root, [validator_dir, archival1_dir, archival2_dir,
                    rpc_dir] = init_cluster(
                        num_nodes=1,
                        num_observers=3,
                        num_shards=1,
                        config=config,
                        genesis_config_changes=genesis_config_changes,
                        client_config_changes=client_config_changes,
                        prefix="test_archival_node_sync_",
                    )

        validator = spin_up_node(
            config,
            near_root,
            validator_dir,
            0,
        )
        archival1 = spin_up_node(
            config,
            near_root,
            archival1_dir,
            1,
            boot_node=validator,
        )
        archival2 = spin_up_node(
            config,
            near_root,
            archival2_dir,
            2,
            boot_node=archival1,
        )
        rpc = spin_up_node(
            config,
            near_root,
            rpc_dir,
            3,
            boot_node=validator,
        )

        # Remember ~where archival node stopped
        n = archival2.get_latest_block().height

        logger.info("Kill the second archival node.")
        # Now, kill the second archival node, so it will be behind after restart and will be forced to sync from the first node.
        archival2.kill()

        logger.info("Wait for the second node to have head only in cold db.")
        # Wait for split storage to rely on cold db to sync archival node
        wait_for_blocks(archival1,
                        target=n + epoch_length * gc_epoch_num * 2 + 1)
        # Kill validator and rpc so the second archival doesn't have any peers that may accidentally have some useful data.
        validator.kill()
        rpc.kill()

        logger.info("Restart the second archival node.")
        # Restart the second archival node. This should trigger sync.
        archival2.start(boot_node=archival1)

        logger.info("Wait for the second archival node to start syncing.")

        # The second archival node should be able to sync to split storage without problems.
        wait_for_blocks(archival2, target=n + epoch_length + 10, timeout=140)
        archival2.kill()
        archival1.kill()


if __name__ == "__main__":
    unittest.main()
