"""
Test case classes for release tests on forknet.
"""
from .base import TestSetup, NodeHardware
from mirror import CommandContext, update_config_cmd, run_remote_cmd
import copy
from utils import ScheduleMode


class TestOnly290(TestSetup):
    """
    Test case:
    - only 2.9.0 from the top of the forknet. no upgrade.
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
        self.genesis_protocol_version = 80
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.9.0_forknet/neard'

    def amend_epoch_config(self):
        super().amend_epoch_config()
        self._amend_epoch_config(
            ".shuffle_shard_assignment_for_chunk_producers = true")


class Test290(TestSetup):
    """
    Test case:
    - upgrade from 2.8.0 to 2.9.0
    """

    def __init__(self, args):
        super().__init__(args)
        self.start_height = 160684617
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=19, num_chunk_validator_seats=24)
        self.epoch_len = 1200  # 1200/1.7/60 = ~11.5min
        self.has_state_dumper = False
        self.genesis_protocol_version = 80
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.upgrade_interval_minutes = 5  # 5 min; within the first 2 epochs
        self.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.8.0/neard'
        # this branch is to iterate faster than the main release branch.
        # keep it in sync with the main release branch.
        self.neard_upgrade_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.9.0_forknet/neard'

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        # TODO: maybe set voting date in ENV to prevent kickouts.

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._schedule_upgrade_nodes_every_n_minutes(
            self.upgrade_interval_minutes)


class Test290Long(Test290):
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
        # mirror schedule cmd --schedule-id "restake" --on 'calendar hourly' run-cmd --cmd "sh .near/neard-runner/send-stake-proposal.sh"
        run_cmd_args = copy.deepcopy(self.args)
        run_cmd_args.host_type = 'nodes'
        # TODO: exclude the dumper and archival nodes
        # run_cmd_args.host_filter = "^(?!.*(dumper|archival))"
        run_cmd_args.schedule_id = "restake"
        run_cmd_args.on = ScheduleMode(mode="calendar", value="hourly")
        run_cmd_args.cmd = "sh .near/neard-runner/send-stake-proposal.sh"
        run_remote_cmd(CommandContext(run_cmd_args))


class Test290Small(Test290):
    """
    Smaller example test case:
    Short epoch length, long blocks.
    Less validators.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=10)
