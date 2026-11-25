"""
Forknet scenario with hosts that are only chunk validators.
"""
from .base import NodeHardware
from .test_release_candidate import TestReleaseCandidate


class TestChunkValidatorHosts(TestReleaseCandidate):
    """
    TestReleaseCandidate with 10 chunk producer hosts and 2 hosts that are only
    chunk validators. Those 2 start without state and download genesis from GCS.
    """

    def __init__(self, args):
        super().__init__(args)
        self.node_hardware_config = NodeHardware.SmallChunkValidatorsConfig(
            num_chunk_producer_seats=10, num_chunk_validator_seats=12)
