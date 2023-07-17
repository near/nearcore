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

V0 = {"V0": {"num_shards": 1, "version": 0}}
V1 = {
    "V1": {
        "boundary_accounts": [
            "aurora", "aurora-0", "kkuuue2akv_1630967379.near"
        ],
        "shards_split_map": [[0, 1, 2, 3]],
        "to_parent_shard_map": [0, 0, 0, 0],
        "version": 1
    }
}

config = load_config()

binary_protocol_version = get_binary_protocol_version(config)
assert binary_protocol_version is not None

V1_PROTOCOL_VERSION = 48
V2_PROTOCOL_VERSION = 135


def get_initial_protocol_version(binary_protocol_version):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        # We'll be testing V1 -> V2 resharding.
        # Set the initial protocol version to a version before V2.
        return V2_PROTOCOL_VERSION - 1

    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        # We'll be testing V0 -> V1 resharding.
        # Set the initial protocol version to a version before V1.
        return V1_PROTOCOL_VERSION - 1

    assert False


def get_initial_shard_layout(binary_protocol_version):
    if binary_protocol_version >= V2_PROTOCOL_VERSION:
        # We'll be testing V1 -> V2 resharding.
        return V1

    if binary_protocol_version >= V1_PROTOCOL_VERSION:
        # We'll be testing V0 -> V1 resharding.
        return V0

    assert False


protocol_version = get_initial_protocol_version(binary_protocol_version)
shard_layout = get_initial_shard_layout(binary_protocol_version)

near_root, node_dirs = init_cluster(
    num_nodes=2,
    num_observers=1,
    num_shards=4,
    config=config,
    genesis_config_changes=[
        ["min_gas_price", 0],
        ["max_inflation_rate", [0, 1]],
        ["epoch_length", EPOCH_LENGTH],
        ["protocol_version", protocol_version],
        ["use_production_config", True],
        ["block_producer_kickout_threshold", 80],
        ["shard_layout", shard_layout],
    ],
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
