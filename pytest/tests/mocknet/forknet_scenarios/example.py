"""
Test case classes for release tests on forknet.
"""
from .base import TestSetup, NodeHardware
from mirror import CommandContext, update_config_cmd
import copy


class Example(TestSetup):
    """
    Example test case:
    - upgrade from 2.7.1 to 2.8.0
    """

    def __init__(self, args):
        super().__init__(args)
        self.start_height = 160684617
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=19, num_chunk_validator_seats=24)
        self.epoch_len = 5000  # roughly 2h 30m @ 2bps
        self.has_state_dumper = True
        self.genesis_protocol_version = 79
        self.has_archival = True
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.upgrade_interval_minutes = 15  # Within the first 2 epochs
        self.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.7.1/neard'
        self.neard_upgrade_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/2.8.0/neard'

    def amend_epoch_config(self):
        super().amend_epoch_config()
        # self._amend_epoch_config(
        #     ".shuffle_shard_assignment_for_chunk_producers = true")

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        # cfg_args = copy.deepcopy(self.args)
        # Reduce the block production delay for this release test.
        # cfg_args.set = ';'.join([
        #     'consensus.min_block_production_delay={"secs":0,"nanos":600000000}',
        #     'consensus.max_block_production_delay={"secs":1,"nanos":800000000}',
        #     'consensus.chunk_wait_mult=[1,3]',
        #     'consensus.doomslug_step_period={"secs":0,"nanos":10000000}',
        # ])
        # update_config_cmd(CommandContext(cfg_args))

    def after_test_start(self):
        """
        Use this event to run any commands after the test is started.
        """
        super().after_test_start()
        self._schedule_upgrade_nodes_every_n_minutes(
            self.upgrade_interval_minutes)


class ExampleSmall(Example):
    """
    Smaller example test case:
    Short epoch length, long blocks.
    Less validators.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=9, num_chunk_validator_seats=10)
