"""
Test case for the release that stabilizes external contract calls.
"""
from .base import TestSetup, NodeHardware


class TestEccRelease(TestSetup):
    """
    Test case: upgrade from the previous release to the release candidate that
    stabilizes external contract calls, then leave the network running so that
    external calls can be sent through the traffic node's RPC.

    Features:
        - One chunk producer per shard, no shard shuffling, so a modified
          producer keeps one shard for the whole run.
        - Archival node, no state dumper.
        - Short epochs and an upgrade vote pinned at 9999/10000, so the
          protocol upgrade lands as soon as every block producer runs the
          new binary.
        - Binary rolled out in 4 batches, `upgrade_interval_minutes` apart.

    Required arguments:
        - neard_binary_url: The URL of the starting neard binary.
        - neard_upgrade_binary_url: The URL of the neard binary to upgrade to.
        - genesis_protocol_version: The starting protocol version to use for the network.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=11)
        self.epoch_len = 500
        self.has_state_dumper = False
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"

        self.upgrade_interval_minutes = 5
        self.upgrade_delay_minutes = 20

    def amend_epoch_config(self):
        super().amend_epoch_config()
        self._amend_epoch_config(
            ".protocol_upgrade_stake_threshold = [9999,10000]")
        self._amend_epoch_config(
            ".shuffle_shard_assignment_for_chunk_producers = false")
        self._amend_epoch_config(".block_producer_kickout_threshold = 0")
        self._amend_epoch_config(".chunk_producer_kickout_threshold = 0")
        self._amend_epoch_config(".chunk_validator_only_kickout_threshold = 0")

    def after_test_start(self):
        super().after_test_start()
        self._schedule_upgrade_nodes_every_n_minutes(
            self.upgrade_interval_minutes)
