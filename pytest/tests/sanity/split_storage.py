#!/usr/bin/python3
"""
 Spins up an archival node with cold store configured and verifies that blocks
 are copied from hot to cold store.
"""

import copy
import json
import os
import os.path as path
import pathlib
import shutil
import subprocess
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
        self.assertGreaterEqual(final_head_height, expected_head_height - 10)
        if check_cold_head:
            self.assertGreaterEqual(cold_head_height, final_head_height - 10)
        else:
            self.assertIsNone(cold_head_height)

    # Migrate archival node using rpc node as source for hot db.
    # wait_period should be enough block for initial migration to finish, cold_head catch up to head and tail to appear.
    # for localnet tests it is enough to use epoch size * number of epochs to keep.
    def _migrate_to_split_storage(self, rpc, archival, archival_dir,
                                  wait_period):
        logger.info("Phase 1 - Running neard before migration.")

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

        logger.info("Phase 2 - Setting the cold storage and restarting neard.")

        self._configure_cold_storage(archival_dir)
        archival.kill()
        archival.start()

        # Wait for a few seconds to:
        # - Give the node enough time to get started.
        # - Give the cold store loop enough time to run - it runs every 1s.
        # TODO(posvyatokum) this is a quick stop-gap solution to fix nayduck, this
        # should be solved by waiting in a loop until cold store head is at
        # expected proximity to final head.
        time.sleep(4)

        # Wait until enough blocks so that we produce enough blocks to fill 5
        # epochs to trigger GC - otherwise the tail won't be set.
        n = max(n, wait_period) + 5
        logger.info(f"Wait until RPC reaches #{n}")
        wait_for_blocks(rpc, target=n)
        logger.info(f"Wait until archival reaches #{n}")
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

        logger.info("Phase 3 - Preparing hot storage from rpc backup.")

        # Stop the RPC node in order to dump the db to disk.
        rpc.kill()

        rpc_src = path.join(rpc.node_dir, "data")
        rpc_dst = path.join(archival.node_dir, "hot_data")
        logger.info(f"Copying rpc backup from {rpc_src} to {rpc_dst}")
        shutil.copytree(rpc_src, rpc_dst)

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

        logger.info("Phase 4 - After migration.")

        archival.kill()
        archival.start()
        rpc.start()

        # Wait for a few seconds to:
        # - Give the node enough time to get started.
        # - Give the cold store loop enough time to run - it runs every 1s.
        # TODO(posvyatokum) this is a quick stop-gap solution to fix nayduck, this
        # should be solved by waiting in a loop until cold store head is at
        # expected proximity to final head.
        time.sleep(4)

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

    # Configure cold storage and start neard. Wait for a few blocks
    # and verify that cold head is moving and that it's close behind
    # final head.
    def step1_base_case_test(self):
        logger.info(f"starting test_base_case")

        config = load_config()
        client_config_changes = {
            0: {
                'archive': True,
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

        self._configure_cold_storage(node_dir)

        node = spin_up_node(config, near_root, node_dir, 0, single_node=True)

        # Wait until enough blocks are produced so that we're guaranteed that
        # cold head has enough time to move and initial migration is finished.
        # The initial migration may fail at the beginning because FINAL_HEAD is
        # not set. It will retry after 30s.
        # TODO it would be better to configure the retry timeout above and set
        # it to a lower value to speed up the test.
        n = 50
        wait_for_blocks(node, target=n)

        self._check_split_storage_info(
            "base_case",
            node=node,
            expected_head_height=n,
            expected_hot_db_kind="Archive",
            check_cold_head=True,
        )

        node.kill()
        logger.info(f"Stopped node0 from base_chase_test")

    # Test the migration from single storage to split storage. This test spins
    # up two nodes, a validator node and an archival node. The validator stays
    # alive for the whole duration of the test. The archival node is migrated
    # to cold storage.
    # - phase 1 - have archival run with single storage for a few blocks
    # - phase 2 - configure cold storage and restart archival
    # - phase 3 - prepare hot storage from a rpc backup
    # - phase 4 - restart archival and check that it's correctly migrated
    def step2_migration_test(self):
        logger.info(f"Starting the migration test")

        # Archival nodes do not run state sync. This means that if peers
        # ran away further than epoch_lenght * gc_epoch_num, archival nodes
        # will not be able to further sync. In practice it means we need a long
        # enough epoch_length or more gc_epoch_num to keep.
        epoch_length = 10
        gc_epoch_num = 3

        genesis_config_changes = [
            ("epoch_length", epoch_length),
        ]
        client_config_changes = {
            # The validator node should be archival and be the source of
            # truth (and sync) for the other nodes. It needs to be archival
            # in case it runs away from the real archival node and the latter
            # won't be able to sync anymore.
            0: {
                'archive': True,
                'tracked_shards': [0],
            },
            # The rpc node will be used as the source of rpc backup db.
            1: {
                'archive': False,
                'tracked_shards': [0],
                'gc_num_epochs_to_keep': gc_epoch_num,
                'state_sync_enabled': True
            },
            # The archival node will be migrated to split storage.
            2: {
                'archive': True,
                'tracked_shards': [0],
                'save_trie_changes': True,
            },
        }

        config = load_config()
        near_root, [validator_dir, rpc_dir, archival_dir] = init_cluster(
            num_nodes=2,
            num_observers=1,
            num_shards=1,
            config=config,
            genesis_config_changes=genesis_config_changes,
            client_config_changes=client_config_changes,
            prefix="test_migration_",
        )

        validator = spin_up_node(
            config,
            near_root,
            validator_dir,
            0,
        )
        rpc = spin_up_node(
            config,
            near_root,
            rpc_dir,
            1,
            boot_node=validator,
        )
        archival = spin_up_node(
            config,
            near_root,
            archival_dir,
            2,
            boot_node=validator,
        )

        self._migrate_to_split_storage(rpc, archival, archival_dir,
                                       gc_epoch_num * epoch_length)

        validator.kill()
        rpc.kill()
        archival.kill()

    # Spin up legacy archival node and split storage node.
    # Pause legacy archival node, but continue running split storage node.
    # After split storage hot tail is bigger than legacy archival head, restart legacy archival node.
    # Make sure that legacy archival node is able to sync after that.
    def step3_archival_node_sync_test(self):
        logger.info(f"Starting the archival <- split storage sync test")

        # Archival nodes do not run state sync. This means that if peers
        # ran away further than epoch_lenght * gc_epoch_num, archival nodes
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
                'tracked_shards': [0],
                'state_sync_enabled': True
            },
            1: {
                'archive': True,
                'tracked_shards': [0],
                'save_trie_changes': True,
                'split_storage': {
                    'enable_split_storage_view_client': True
                },
            },
            2: {
                'archive': True,
                'tracked_shards': [0],
            },
            3: {
                'archive': False,
                'tracked_shards': [0],
                'gc_num_epochs_to_keep': gc_epoch_num,
                'state_sync_enabled': True
            },
        }
        config = load_config()
        near_root, [validator_dir, split_dir, archival_dir,
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
        split = spin_up_node(
            config,
            near_root,
            split_dir,
            1,
            boot_node=validator,
        )
        archival = spin_up_node(
            config,
            near_root,
            archival_dir,
            2,
            boot_node=split,
        )
        rpc = spin_up_node(
            config,
            near_root,
            rpc_dir,
            3,
            boot_node=validator,
        )

        # First, migrate split node to split storage
        self._migrate_to_split_storage(rpc, split, split_dir,
                                       gc_epoch_num * epoch_length)

        # Remember ~where archival node stopped
        n = archival.get_latest_block().height

        logger.info("Kill legacy archival node.")
        # Now, kill legacy archival node, so it will be behind after restart and will be forced to sync from split node.
        archival.kill()

        logger.info(
            "Wait for split storage to have legacy archival head only in cold db."
        )
        # Wait for split storage to relly on cold db to sync archival node
        wait_for_blocks(split, target=n + epoch_length * gc_epoch_num * 2 + 1)
        # Kill validator and rpc so legacy archival doesn't have any peers that may accidentally have some useful data.
        validator.kill()
        rpc.kill()

        logger.info("Restart legacy archival node.")
        # Restart archival node. This should trigger sync.
        archival.start(boot_node=split)
        time.sleep(10)

        logger.info("Wait for legacy archival node to start syncing.")

        # Archival node should be able to sync to split storage without problems.
        wait_for_blocks(archival, target=n + epoch_length, timeout=120)
        archival.kill()
        split.kill()


if __name__ == "__main__":
    unittest.main()
