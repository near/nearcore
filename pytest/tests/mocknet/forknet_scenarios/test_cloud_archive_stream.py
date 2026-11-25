"""
Forknet scenario for testing the cloud archive as the traffic generator's block source.
"""
from .base import TestSetup, NodeHardware


class TestCloudArchiveStream(TestSetup):
    """
    Small forknet for iterating on the mirror tool with a cloud archive source.
    - 10 chunk producers, one host each, no validator-only hosts.
    - All hosts, the traffic host too, in europe-west4, the region of the
      mainnet cloud archive bucket.
    - No shard shuffling, no archival nodes, no state dumper.
    - Epoch length 14500 (about 2h at forknet block speed).
    - No upgrade step. The neard binary comes from --neard-binary-url and must
      have `neard mirror run --source-cloud-archive`; a binary without it
      leaves the forknet with no traffic.
    """

    def __init__(self, args):
        super().__init__(args)
        # The mainnet bucket holds every block from 220204320 up.
        self.start_height = 220587532
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=10, num_chunk_validator_seats=10)
        self.epoch_len = 14500
        self.has_state_dumper = False
        self.has_archival = False
        self.tracing_server = False
        self.regions = "europe-west4"
        self.genesis_protocol_version = 88
        # The traffic generator streams the source chain out of the cloud
        # archive bucket named in the reader home's config.json. The reader
        # home must already exist on the traffic host (config.json,
        # genesis.json and node_key.json; its store is bootstrapped from the
        # bucket on the first start, without state). A later improvement is for
        # the runner to create it from the source genesis before the source disk
        # goes away.
        self.source_cloud_archive = True
        self.source_home = '/home/ubuntu/reader2'
