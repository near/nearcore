"""
Test case classes for release tests on forknet.
"""
from .base import TestSetup
import copy


class Test27(TestSetup):
    """
    Test case 2.7:
    - upgrade from 2.6.3 to 2.7
    """

    def __init__(self, args):
        args.neard_binary_url = 'https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux/2.6.3_forknet/neard'
        super().__init__(args)
        self.start_height = 149655594
        self.args.start_height = self.start_height
        self.validators = 21
        self.block_producers = 18
        self.epoch_len = 18000  # roughly 2h 30m @ 2bps
        self.genesis_protocol_version = 77
        self.has_archival = True
        self.regions = None
        self.upgrade_interval_minutes = 40  # Within the first 2 epochs

    def amend_configs(self):
        super().amend_configs()
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
        self._schedule_upgrade_nodes_every_n_minutes(
            self.upgrade_interval_minutes)


class Test27Small(Test27):
    """
    Smaller test case for 2.7:
    Short epoch length, long blocks.
    Less validators.
    """

    def __init__(self, args):
        super().__init__(args)
        self.validators = 9
        self.block_producers = 8
        self.epoch_len = 5000
        self.has_archival = True
        self.regions = None
        self.upgrade_interval_minutes = 5  # Within the first 2 epochs

    def amend_configs(self):
        super().amend_configs()
        cfg_args = copy.deepcopy(self.args)
        cfg_args.set = ';'.join([
            'consensus.min_block_production_delay={"secs":1,"nanos":300000000}',
            'consensus.max_block_production_delay={"secs":3,"nanos":0}',
            'consensus.chunk_wait_mult=[1,6]',
            'consensus.doomslug_step_period={"secs":0,"nanos":100000000}'
        ])
        update_config_cmd(CommandContext(cfg_args))
