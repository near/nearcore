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
import subprocess
import shutil
import os.path as path

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from utils import wait_for_blocks
from cluster import init_cluster, spin_up_node, load_config, get_config_json, set_config_json
from configured_logger import logger


class TestSplitStorage(unittest.TestCase):

    def _pretty_json(self, value):
        return json.dumps(value, indent=2)

    def _get_split_storage_info(self, node):
        return node.json_rpc("EXPERIMENTAL_split_storage_info", {})

    def _configure_cold_storage(self, node_dir):
        node_config = get_config_json(node_dir)

        # Need to create a deepcopy of the store config, otherwise
        # store and cold store will point to the same instance and
        # both will end up with the same path.
        node_config["cold_store"] = copy.deepcopy(node_config["store"])
        node_config["store"]["path"] = path.join(node_dir, 'data')
        node_config["cold_store"]["path"] = path.join(node_dir, 'cold_data')

        set_config_json(node_dir, node_config)

    def _configure_hot_storage(self, node_dir, new_path):
        node_config = get_config_json(node_dir)
        node_config["store"]["path"] = new_path
        set_config_json(node_dir, node_config)

    def _check_split_storage_info(
        self,
        msg,
        node,
        expected_head_height,
        expected_hot_db_kind,
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

        self.assertEqual(hot_db_kind, expected_hot_db_kind)
        self.assertGreaterEqual(head_height, expected_head_height)
        self.assertGreaterEqual(final_head_height, expected_head_height - 3)
        if check_cold_head:
            self.assertGreaterEqual(cold_head_height, final_head_height - 3)
        else:
            self.assertIsNone(cold_head_height)

    # Configure cold storage and start neard. Wait for a few blocks
    # and verify that cold head is moving and that it's close behind
    # final head.
    def test_base_case(self):
        print()
        logger.info(f"starting test_base_case")

        config = load_config()
        client_config_changes = {
            0: {
                'archive': True
            },
        }
        near_root, [node_dir] = init_cluster(
            1,
            0,
            1,
            config,
            [],
            client_config_changes,
            "test_base_case_",
        )

        self._configure_cold_storage(node_dir)

        node = spin_up_node(config, near_root, node_dir, 0, single_node=True)

        # Wait until 20 blocks are produced so that we're guaranteed that
        # cold head has enough time to move. cold_head <= final_head < head
        n = 20
        wait_for_blocks(node, target=n)

        self._check_split_storage_info(
            "base_case",
            node=node,
            expected_head_height=n,
            expected_hot_db_kind="Archive",
            check_cold_head=True,
        )

        node.kill(gentle=True)

    # Test the migration from single storage to split storage. This test spins
    # up two nodes, a validator node and an archival node. The validator stays
    # alive for the whole duration of the test. The archival node is migrated
    # to cold storage.
    # - phase 1 - have archival run with single storage for a few blocks
    # - phase 2 - configure cold storage and restart archival
    # - phase 3 - prepare hot storage from a rpc backup
    # - phase 4 - restart archival and check that it's correctly migrated
    def test_migration(self):
        print()
        logger.info(f"Starting the migration test")
        logger.info("")

        # Decrease the epoch length and gc epoch num in order to speed up this test.
        epoch_length = 4
        gc_epoch_num = 3

        config = load_config()
        near_root, [validator_dir, archival_dir] = init_cluster(
            num_nodes=1,
            num_observers=1,
            num_shards=1,
            config=config,
            genesis_config_changes=[
                ("epoch_length", epoch_length),
            ],
            client_config_changes={
                # The validator node should not be archival so that we can use
                # its db as the RPC backup for the archival node migration.
                0: {
                    'archive': False,
                    'tracked_shards': [0],
                    'gc_num_epochs_to_keep': gc_epoch_num,
                },
                # The archival node should be archival so that it's archival.
                1: {
                    'archive': True,
                    'tracked_shards': [0],
                },
            },
            prefix="test_migration_",
        )

        logger.info("")
        logger.info("Phase 1 - Starting neard before migration.")
        logger.info("")

        validator = spin_up_node(
            config,
            near_root,
            validator_dir,
            0,
        )
        archival = spin_up_node(
            config,
            near_root,
            archival_dir,
            1,
            boot_node=validator,
        )

        # Wait until a few blocks are produced so that we're sure that the db is
        # properly created and populated with some data.
        n = 5
        n = wait_for_blocks(archival, target=n).height

        self._check_split_storage_info(
            "migration_phase_1",
            node=archival,
            expected_head_height=n,
            # The hot db kind should remain archive until fully migrated.
            expected_hot_db_kind="Archive",
            # The cold storage is not configured so cold head should be none.
            check_cold_head=False,
        )

        logger.info("")
        logger.info("Phase 2 - Setting the cold storage and restarting neard.")
        logger.info("")

        self._configure_cold_storage(archival_dir)
        archival.kill(gentle=True)
        archival.start()

        # Wait until enough blocks are produced so that cold store loop has enough
        # time to catch up and so that we produce enough blocks to fill 5 epochs
        # to trigger GC - otherwise the tail won't be set.
        n = max(n, gc_epoch_num * epoch_length) + 5
        wait_for_blocks(archival, target=n)

        self._check_split_storage_info(
            "migration_phase_2",
            node=archival,
            expected_head_height=n,
            # The hot db kind should remain archive until fully migrated.
            expected_hot_db_kind="Archive",
            # The cold storage head should be fully caught up by now.
            check_cold_head=True,
        )

        logger.info("")
        logger.info("Phase 3 - Preparing hot storage from rpc backup.")
        logger.info("")

        rpc_src = path.join(validator.node_dir, "data")
        rpc_dst = path.join(archival.node_dir, "hot_data")
        logger.info(f"Copying rpc backup from {rpc_src} to {rpc_dst}")
        shutil.copytree(rpc_src, rpc_dst)

        # TODO Ideally we won't need to stop the node while running prepare-hot.
        archival.kill(gentle=True)

        archival_dir = pathlib.Path(archival.node_dir)
        with open(archival_dir / 'prepare-hot-stdout', 'w') as stdout, \
             open(archival_dir / 'prepare-hot-stderr', 'w') as stderr:
            cmd = [
                str(pathlib.Path(archival.near_root) / archival.binary_name),
                f'--home={archival_dir}',
                f'cold-store',
                f'prepare-hot',
                f'--store-relative-path',
                f'hot_data',
            ]
            logger.info(f"Calling '{' '.join(cmd)}'")
            subprocess.check_call(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                env=dict(
                    os.environ,
                    RUST_LOG='debug',
                ),
            )

        self._configure_hot_storage(archival_dir, rpc_dst)

        logger.info("")
        logger.info("Phase 4 - After migration.")
        logger.info("")

        archival.start()

        # Wait for just a few blocks to make sure neard correctly restarted.
        n += 5
        wait_for_blocks(archival, target=n)

        self._check_split_storage_info(
            "migration_phase_4",
            node=archival,
            expected_head_height=n,
            # The migration is over, the hot db kind should be set to hot.
            expected_hot_db_kind="Hot",
            # The cold storage head should be fully caught up.
            check_cold_head=True,
        )


if __name__ == "__main__":
    unittest.main()
