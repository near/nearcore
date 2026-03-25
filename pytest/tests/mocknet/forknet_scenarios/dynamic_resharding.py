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
        self.epoch_len = 20000  # ~3h
        self.has_state_dumper = False
        self.genesis_protocol_version = 84
        self.has_archival = False
        self.regions = "us-east1,europe-west4,asia-east1,us-west1"
        self.neard_binary_url = "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/wiezzel/dynamic-resharding-test/73c58cff46f4d0a7024afa5f14494364eac7dba8/release/neard"
