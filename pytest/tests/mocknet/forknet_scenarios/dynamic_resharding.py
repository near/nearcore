"""
Test case classes for release tests on forknet.
"""
from .base import TestSetup, NodeHardware
from mirror import CommandContext, update_config_cmd


class DynamicResharding(TestSetup):

    def __init__(self, args):
        super().__init__(args)
        self.start_height = 180121999  # 2_10_release
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=20, num_chunk_validator_seats=20)
        self.epoch_len = 6000  # ~3h
        self.has_state_dumper = False
        self.genesis_protocol_version = 84
        self.has_archival = False
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.neard_binary_url = "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/wiezzel/dynamic-resharding-test/08991eab412b19ce1f3ec699e8ec8c1acd95f9ba/release/neard"

    def amend_epoch_config(self):
        super().amend_epoch_config()

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._schedule_upgrade_nodes_every_n_minutes(
            self.upgrade_interval_minutes)
