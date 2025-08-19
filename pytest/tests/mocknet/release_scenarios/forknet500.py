"""
Test case classes for release tests on forknet.
"""
from .base import NodeHardware, TestSetup
from mirror import CommandContext, update_config_cmd
import copy


class Test28(TestSetup):
    """
    Test case 2.8 on 500 nodes:
    - upgrade start from 2.7 and upgrade to 2.8
    """

    def __init__(self, args):
        # args.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.7.0_forknet/neard'
        args.neard_binary_url = 'https://storage.googleapis.com/logunov-s/neard-0819'
        super().__init__(args)
        self.start_height = 158710624
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SmallChunkValidatorsConfig(
            num_chunk_producer_seats=1, num_chunk_validator_seats=2)
        self.epoch_len = 200
        self.has_state_dumper = False
        self.genesis_protocol_version = 80  # 79
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

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        #self._schedule_upgrade_nodes_every_n_minutes(
        #    self.upgrade_interval_minutes)


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
