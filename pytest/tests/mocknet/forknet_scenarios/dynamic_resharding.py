"""
Test case classes for release tests on forknet.
"""
from .base import TestSetup, NodeHardware
from mirror import CommandContext, update_config_cmd


class DynamicResharding(TestSetup):
    """
    Test case:
    - exercises dynamic resharding so the new dynamic-resharding metrics
      (resharding decision/orchestration + background memtrie load) light up.
    - pins sticky (non-shuffled) chunk-producer-to-shard assignment so the
      background memtrie load for the upcoming child shards can run; forknet
      otherwise inherits shuffling from the mainnet base epoch config.
    """

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
        self.regions = "europe-west4,us-east1,us-west1"
        # Custom build of wiezzel/forknet-preserve-dynamic-resharding (commit
        # 091936bd8 = latest master + the dynamic-resharding metric improvements +
        # the fork-network fix that preserves the Dynamic shard-layout config, so
        # resharding triggers natively with no epoch-config amend). Built via
        # neard_custom_binary.yml from the nearcore-private repo and uploaded to S3.
        self.neard_binary_url = "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/wiezzel/forknet-preserve-dynamic-resharding/091936bd81ed5c6bfcd990262371637918969584/release/neard"

    def amend_epoch_config(self):
        super().amend_epoch_config()
        # Force sticky validator assignment. The mainnet base epoch config
        # turns shard-assignment shuffling on at protocol v143, so without this
        # override a chunk producer can be moved off the parent shard right when
        # resharding splits it, breaking the background memtrie pre-load.
        self._amend_epoch_config(
            ".shuffle_shard_assignment_for_chunk_producers = false")
