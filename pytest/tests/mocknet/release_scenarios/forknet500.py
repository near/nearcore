"""
Test case classes for release tests on forknet.
"""
from .base import NodeHardware, TestSetup
from mirror import CommandContext, ScheduleMode, update_config_cmd, run_remote_cmd, start_nodes_cmd
import copy
from datetime import datetime, timedelta, timezone


class Test28(TestSetup):
    """
    Test case 2.8 on 500 nodes:
    - upgrade start from 2.7 and upgrade to 2.8
    """

    def __init__(self, args):
        # args.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.7.0_forknet/neard'
        # args.neard_binary_url = 'https://storage.googleapis.com/logunov-s/neard-0819'
        # args.neard_binary_url = 'https://storage.googleapis.com/logunov-s/neard-0825'
        args.neard_binary_url = 'https://storage.googleapis.com/logunov-s/neard-2.7.0-cv-fix'
        args.neard_upgrade_binary_url = 'https://storage.googleapis.com/logunov-s/neard-2.8.0-cv-fix'
        super().__init__(args)
        self.start_height = 158710624
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SmallChunkValidatorsConfig(
            num_chunk_producer_seats=100, num_chunk_validator_seats=500)
        self.epoch_len = 400
        self.has_state_dumper = False
        self.genesis_protocol_version = 79
        self.has_archival = False
        self.regions = "us-east1,europe-west4,asia-east1,us-west1,asia-south1,europe-west1,asia-southeast1"
        self.upgrade_interval_minutes = 5  # Within the first 2 epochs

    def amend_epoch_config(self):
        super().amend_epoch_config()
        #self._amend_epoch_config(
        #    f".shuffle_shard_assignment_for_chunk_producers = true")

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        cfg_args = copy.deepcopy(self.args)
        # Reduce the block production delay for this release test.
        # These configs will be default in 2.7
        cfg_args.set = ';'.join([
            'consensus.min_block_production_delay={"secs":0,"nanos":600000000}',
            'consensus.max_block_production_delay={"secs":1,"nanos":800000000}',
            'consensus.chunk_wait_mult=[1,3]',
            'consensus.doomslug_step_period={"secs":0,"nanos":10000000}',
        ])
        update_config_cmd(CommandContext(cfg_args))

    def _schedule_upgrade_nodes_every_n_minutes(self, minutes):

        def time_to_str(time):
            return time.astimezone(
                timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

        now = datetime.now()
        ref_time = [now + timedelta(minutes=i * minutes) for i in range(1, 5)]

        for i in range(1, 5):
            upgrade_time_str = time_to_str(ref_time[i - 1])

            # Start node with the new binary
            start_nodes_args = copy.deepcopy(self.args)
            start_nodes_args.host_type = 'nodes'
            start_nodes_args.select_partition = (i, 4)
            start_nodes_args.on = ScheduleMode(mode="calendar",
                                               value=upgrade_time_str)
            start_nodes_args.schedule_id = f"up-start-{i}"
            start_nodes_args.binary_idx = 1
            start_nodes_cmd(CommandContext(start_nodes_args))

            stake_time_str = time_to_str(ref_time[i - 1] +
                                         timedelta(minutes=2 * minutes))

            # Send stake transaction to RPC node using node key
            stake_cmd_args = copy.deepcopy(self.args)
            stake_cmd_args.host_filter = '-cv-'
            stake_cmd_args.select_partition = (i, 4)
            stake_cmd_args.on = ScheduleMode(mode="calendar", value="*:0/2")
            stake_cmd_args.schedule_id = f"up-stake-{i}"
            stake_cmd_args.cmd = "if pgrep -f neard1 > /dev/null; then bash ~/.near/neard-runner/send-stake-proposal.sh; fi"
            run_remote_cmd(CommandContext(stake_cmd_args))

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._schedule_upgrade_nodes_every_n_minutes(
            self.upgrade_interval_minutes)


class OneNode(TestSetup):
    """
    Test case 1 node:
    - upgrade start from 2.7 and upgrade to 2.8
    """

    def __init__(self, args):
        args.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.7.0_forknet/neard'
        super().__init__(args)
        self.start_height = 158710624
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SmallChunkValidatorsConfig(
            num_chunk_producer_seats=1, num_chunk_validator_seats=2)
        self.epoch_len = 300
        self.has_state_dumper = False
        self.genesis_protocol_version = 79
        self.has_archival = False
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.upgrade_interval_minutes = 60  # Within the first 2 epochs

    def amend_epoch_config(self):
        super().amend_epoch_config()
        #self._amend_epoch_config(
        #    f".shuffle_shard_assignment_for_chunk_producers = true")

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        cfg_args = copy.deepcopy(self.args)
        # Reduce the block production delay for this release test.
        # These configs will be default in 2.7
        cfg_args.set = ';'.join([
            'consensus.min_block_production_delay={"secs":0,"nanos":600000000}',
            'consensus.max_block_production_delay={"secs":1,"nanos":800000000}',
            'consensus.chunk_wait_mult=[1,3]',
            'consensus.doomslug_step_period={"secs":0,"nanos":10000000}',
        ])
        update_config_cmd(CommandContext(cfg_args))

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        #self._schedule_upgrade_nodes_every_n_minutes(
        #    self.upgrade_interval_minutes)
