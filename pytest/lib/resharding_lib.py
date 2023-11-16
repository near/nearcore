# A library with the common constants and functions for testing resharding.

V1_PROTOCOL_VERSION = 48
V2_PROTOCOL_VERSION = 135

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
