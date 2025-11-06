from .base import TestSetup, NodeHardware
from mirror import CommandContext, run_remote_cmd
import copy
from utils import ScheduleMode

# this branch is to iterate faster than the main release branch, keep it in sync with the main release branch.
NEARD_BINARY_URL = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.10-forknet/neard'

class TestOnly210(TestSetup):
    """
    Test case:
    - only 2.10 from the top of the forknet, no upgrade
    - shard shuffle for chunk producers
    """

    def __init__(self, args):
        super().__init__(args)
        self.start_height = 160684617
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=19, num_chunk_validator_seats=24)
        self.epoch_len = 15000  # 15000 blocks / 2 bps / 60 / 60 = 2h 5m
        self.has_state_dumper = False
        self.genesis_protocol_version = 81
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.neard_binary_url = NEARD_BINARY_URL

    def amend_epoch_config(self):
        super().amend_epoch_config()
        self._amend_epoch_config(
            ".shuffle_shard_assignment_for_chunk_producers = true")


class Test210(TestSetup):
    """
    Test case:
    - upgrade from 2.9 to 2.10
    """

    def __init__(self, args):
        super().__init__(args)
        self.start_height = 160684617
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=19, num_chunk_validator_seats=24)
        self.epoch_len = 1200  # 1200/1.7/60 = ~11.5min
        self.has_state_dumper = False
        self.genesis_protocol_version = 81
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.upgrade_interval_minutes = 5  # 5 min; within the first 2 epochs
        self.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.9.0/neard'
        self.neard_upgrade_binary_url = NEARD_BINARY_URL 

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._schedule_upgrade_nodes_every_n_minutes(
            self.upgrade_interval_minutes)


class Test210Long(Test210):
    """
    Long running test case:
    Long epoch length, to check state sync before and after the upgrade.
    """

    def __init__(self, args):
        super().__init__(args)
        self.epoch_len = 18000  # 18000 blocks / 2 bps / 60 / 60 = 2.5h
        self.upgrade_delay_minutes = 450  # 450 minutes = 7.5 hours ~= 3 epochs
        self.upgrade_interval_minutes = 20

    def amend_epoch_config(self):
        super().amend_epoch_config()
        self._amend_epoch_config(
            ".shuffle_shard_assignment_for_chunk_producers = true")

    def after_test_start(self):
        super().after_test_start()


class Test210Small(Test210):
    """
    Smaller example test case:
    Short epoch length, long blocks.
    Less validators.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=10)
