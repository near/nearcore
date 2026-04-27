"""
Test case classes for release tests on forknet.
"""
from .base import TestSetup, NodeHardware
from mirror import CommandContext, update_config_cmd

import copy
from utils import PartitionSelector
from datetime import datetime, timedelta


class TestReleaseCandidate(TestSetup):
    """
    Test case:
    - Runs an upgrade test from the previous release to the current release candidate.
    Features:
        - No state dumper.
        - Upgrade happens over 2 epochs.
        - 1 producer per shard
        - 2 validators.

    Required arguments:
        - neard_binary_url: The URL of the starting neard binary.
        - neard_upgrade_binary_url: The URL of the neard binary to upgrade to.
        - genesis_protocol_version: The starting protocol version to use for the network.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=11)
        self.epoch_len = 2000  # 14500 blocks / 2 bps / 60 / 60 = 2h
        self.has_state_dumper = False
        self.regions = "us-east1,europe-west1,asia-east1,us-west1"

        # Upgrade 1/2 nodes at a time, a quarter at a time.
        self.upgrade_interval_minutes = 15  # 15 minutes between each upgrade batch.
        self.first_upgrade_delay_minutes = 45
        self.second_upgrade_delay_minutes = 75

    def amend_epoch_config(self):
        """
        This is a workaround to set the protocol upgrade threshold close to 1, so that the upgrade happens when all the **block producers** have upgraded.
        This avoid the need to properly calculate the upgrade timeline based on the epoch length and the upgrade interval.
        """
        super().amend_epoch_config()
        self._amend_epoch_config(
            f".protocol_upgrade_stake_threshold = [9999,10000]")

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        cfg_args = copy.deepcopy(self.args)
        cfg_args.set = 'save_invalid_witnesses=true'
        update_config_cmd(CommandContext(cfg_args))

    def _upgrade_nodes_in_four_batches(self):
        """
        Upgrade the nodes in two steps, upgrade_delay_minutes apart.
        At each step, we upgrade half of the nodes at a time.
        In total, we upgrade in 4 batches.
        """
        first_upgrade_time = datetime.now() + timedelta(
            minutes=self.first_upgrade_delay_minutes)
        second_upgrade_time = datetime.now() + timedelta(
            minutes=self.second_upgrade_delay_minutes)

        upgrade_time = [
            batch_start_time +
            timedelta(minutes=i * self.upgrade_interval_minutes)
            for batch_start_time in [first_upgrade_time, second_upgrade_time]
            for i in range(0, 2)
        ]
        batches = len(upgrade_time)
        for quarter, batch_start_time in enumerate(upgrade_time):
            self.schedule_binary_upgrade(batch_start_time,
                                         0,
                                         binary_idx=1,
                                         partition=PartitionSelector(
                                             partitions_range=(quarter + 1,
                                                               quarter + 1),
                                             total_partitions=batches))

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._upgrade_nodes_in_four_batches()


class TestReleaseCandidateFast(TestReleaseCandidate):
    """
    Variant of TestReleaseCandidate optimized for protocol-transition
    wall-clock time. Useful for smoke tests; not representative of a real
    release rollout.

    Differences from TestReleaseCandidate:
        - Shorter epochs (epoch_len=500). The new protocol activates 2 epoch
          boundaries after the vote, so epoch length dominates the post-
          upgrade tail.
        - Single-batch upgrade a few minutes after start, instead of four
          staged batches spread over ~45 minutes. The 99.99% stake threshold
          inherited from the parent is still fine because all nodes flip at
          once.
    """

    def __init__(self, args):
        super().__init__(args)
        self.epoch_len = 500
        self.first_upgrade_delay_minutes = 5

    def after_test_start(self):
        # Skip TestReleaseCandidate's 4-batch rollout; upgrade all nodes once.
        TestSetup.after_test_start(self)
        upgrade_time = datetime.now() + timedelta(
            minutes=self.first_upgrade_delay_minutes)
        self.schedule_binary_upgrade(upgrade_time, 0, binary_idx=1)
