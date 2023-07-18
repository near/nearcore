#!/usr/bin/env python3

# Spins up a node, waits until sharding is upgraded and spins up another node.
# Check that the node can't be started because it cannot state sync to the epoch
# after the sharding upgrade.

# Depending on the version of the binary (default or nightly) it will perform
# resharding from V0 (1 shard) to V1 (4 shards) or from V1 (4 shards) to V2 (5
# shards).

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config, get_binary_protocol_version
from configured_logger import logger
import requests
import utils

EPOCH_LENGTH = 10
START_AT_BLOCK = int(EPOCH_LENGTH * 2.5)

V1_PROTOCOL_VERSION = 48
V2_PROTOCOL_VERSION = 135

V0_SHARD_LAYOUT = {"V0": {"num_shards": 1, "version": 0}}
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


def append_shard_layout_config_changes(
    binary_protocol_version,
    genesis_config_changes,
):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
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
        print(genesis_config_changes)
        return

    if binary_protocol_version >= V1_PROTOCOL_VERSION:
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
        print(genesis_config_changes)
        return

    assert False


def get_genesis_config_changes(binary_protocol_version):
    genesis_config_changes = [
        ["min_gas_price", 0],
        ["max_inflation_rate", [0, 1]],
        ["epoch_length", EPOCH_LENGTH],
        ["use_production_config", True],
        ["block_producer_kickout_threshold", 80],
    ]

    append_shard_layout_config_changes(
        binary_protocol_version,
        genesis_config_changes,
    )

    print(genesis_config_changes)

    return genesis_config_changes


config = load_config()

binary_protocol_version = get_binary_protocol_version(config)
assert binary_protocol_version is not None

near_root, node_dirs = init_cluster(
    num_nodes=2,
    num_observers=1,
    num_shards=4,
    config=config,
    genesis_config_changes=get_genesis_config_changes(binary_protocol_version),
    client_config_changes={
        0: {
            "tracked_shards": [0],
            "state_sync_enabled": True,
            "store.state_snapshot_enabled": True,
        },
        1: {
            "tracked_shards": [0],
            "state_sync_enabled": True,
            "store.state_snapshot_enabled": True,
        },
        2: {
            "tracked_shards": [0],
            "consensus": {
                "block_fetch_horizon": EPOCH_LENGTH * 2,
            },
            "state_sync_enabled": True,
            "store.state_snapshot_enabled": True,
        }
    },
)

started = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)

ctx = utils.TxContext([0, 0], [boot_node, node1])

utils.wait_for_blocks(boot_node, target=START_AT_BLOCK)

node2 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=boot_node)
tracker = utils.LogTracker(node2)
time.sleep(3)

try:
    status = node2.get_status()
    sys.exit("node 2 successfully started while it should fail")
except requests.exceptions.ConnectionError:
    pass
