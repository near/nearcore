# A library with the common constants and functions for testing resharding.

# TODO(resharding) The resharding V2 is now stabilized so the V1 code is no
# longer exercised and can be removed.
import unittest

from cluster import get_binary_protocol_version, load_config
from utils import MetricsTracker

V1_PROTOCOL_VERSION = 48
V2_PROTOCOL_VERSION = 64

V0_SHARD_LAYOUT = {
    "V0": {
        "num_shards": 1,
        "version": 0
    },
}
V1_SHARD_LAYOUT = {
    "V1": {
        "boundary_accounts": [
            "aurora", "aurora-0", "kkuuue2akv_1630967379.near"
        ],
        "shards_split_map": [[0, 1, 2, 3]],
        "to_parent_shard_map": [0, 0, 0, 0],
        "version": 1
    }
}


def get_genesis_config_changes(epoch_length,
                               binary_protocol_version,
                               logger=None):
    genesis_config_changes = [
        ["epoch_length", epoch_length],
    ]
    append_shard_layout_config_changes(
        genesis_config_changes,
        binary_protocol_version,
        logger,
    )
    return genesis_config_changes


# Append the genesis config changes that are required for testing resharding.
# This method will set the protocol version, shard layout and a few other
# configs so that it matches the protocol configuration as of right before the
# protocol version of the binary under test.
def append_shard_layout_config_changes(
    genesis_config_changes,
    binary_protocol_version,
    logger=None,
):
    genesis_config_changes.append(["use_production_config", True])

    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        if logger:
            logger.info("Testing migration from V1 to V2.")
        # Set the initial protocol version to a version just before V2.
        genesis_config_changes.append([
            "protocol_version",
            V2_PROTOCOL_VERSION - 1,
        ])
        genesis_config_changes.append([
            "shard_layout",
            V1_SHARD_LAYOUT,
        ])
        genesis_config_changes.append([
            "num_block_producer_seats_per_shard",
            [1, 1, 1, 1],
        ])
        genesis_config_changes.append([
            "avg_hidden_validator_seats_per_shard",
            [0, 0, 0, 0],
        ])
        return

    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        if logger:
            logger.info("Testing migration from V0 to V1.")
        # Set the initial protocol version to a version just before V1.
        genesis_config_changes.append([
            "protocol_version",
            V1_PROTOCOL_VERSION - 1,
        ])
        genesis_config_changes.append([
            "shard_layout",
            V0_SHARD_LAYOUT,
        ])
        genesis_config_changes.append([
            "num_block_producer_seats_per_shard",
            [100],
        ])
        genesis_config_changes.append([
            "avg_hidden_validator_seats_per_shard",
            [0],
        ])
        return

    assert False


def get_genesis_shard_layout_version(binary_protocol_version):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        return 1
    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        return 0

    assert False


def get_target_shard_layout_version(binary_protocol_version):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        return 2
    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        return 1

    assert False


def get_genesis_num_shards(binary_protocol_version):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        return 4
    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        return 1

    assert False


def get_target_num_shards(binary_protocol_version):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        return 5
    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        return 4

    assert False


def get_epoch_offset(binary_protocol_version):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        return 1
    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        return 0

    assert False


def get_client_config_changes(num_nodes, initial_delay=None):
    single = {
        "tracked_shards": [0],
        "resharding_config": {
            "batch_size": 1000000,
            # don't throttle resharding
            "batch_delay": {
                "secs": 0,
                "nanos": 0,
            },
            # retry often to start resharding as fast as possible
            "retry_delay": {
                "secs": 0,
                "nanos": 100_000_000
            }
        }
    }
    if initial_delay is not None:
        single["resharding_config"]["initial_delay"] = {
            "secs": initial_delay,
            "nanos": 0
        }
    return {i: single for i in range(num_nodes)}


class ReshardingTestBase(unittest.TestCase):

    def setUp(self, epoch_length):
        self.epoch_length = epoch_length
        self.config = load_config()
        self.binary_protocol_version = get_binary_protocol_version(self.config)
        assert self.binary_protocol_version is not None

        self.genesis_shard_layout_version = get_genesis_shard_layout_version(
            self.binary_protocol_version)
        self.target_shard_layout_version = get_target_shard_layout_version(
            self.binary_protocol_version)

        self.genesis_num_shards = get_genesis_num_shards(
            self.binary_protocol_version)
        self.target_num_shards = get_target_num_shards(
            self.binary_protocol_version)

        self.epoch_offset = get_epoch_offset(self.binary_protocol_version)

    def get_metric(self, metrics_tracker: MetricsTracker, metric_name):
        return metrics_tracker.get_int_metric_value(metric_name)

    def get_version(self, metrics_tracker: MetricsTracker):
        return self.get_metric(metrics_tracker, "near_shard_layout_version")

    def get_num_shards(self, metrics_tracker: MetricsTracker):
        return self.get_metric(metrics_tracker, "near_shard_layout_num_shards")

    def get_resharding_status(self, metrics_tracker: MetricsTracker):
        return metrics_tracker.get_metric_all_values("near_resharding_status")

    def get_flat_storage_head(self, metrics_tracker: MetricsTracker):
        return metrics_tracker.get_metric_all_values("flat_storage_head_height")
