#!/usr/bin/env python3

# Spins up 2 nodes, waits until sharding is upgraded and spins up another node.
# Check that the node can't be started because it cannot state sync to the epoch
# after the sharding upgrade.

# Depending on the version of the binary (default or nightly) it will perform
# resharding from V0 (1 shard) to V1 (4 shards) or from V1 (4 shards) to V2 (5
# shards).

import pathlib
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config, get_binary_protocol_version
from configured_logger import logger
import requests
import resharding_lib
import state_sync_lib
import utils

EPOCH_LENGTH = 20
START_AT_BLOCK = int(EPOCH_LENGTH * 2.5)


def get_genesis_config_changes(binary_protocol_version):
    genesis_config_changes = [
        ["min_gas_price", 0],
        ["max_inflation_rate", [0, 1]],
        ["epoch_length", EPOCH_LENGTH],
        ["block_producer_kickout_threshold", 80],
    ]

    resharding_lib.append_shard_layout_config_changes(
        genesis_config_changes,
        binary_protocol_version,
        logger,
    )

    return genesis_config_changes


config = load_config()

binary_protocol_version = get_binary_protocol_version(config)
assert binary_protocol_version is not None

node_config = state_sync_lib.get_state_sync_config_combined()

near_root, node_dirs = init_cluster(
    num_nodes=2,
    num_observers=1,
    num_shards=4,
    config=config,
    genesis_config_changes=get_genesis_config_changes(binary_protocol_version),
    client_config_changes={x: node_config for x in range(3)},
)

started = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)

utils.wait_for_blocks(boot_node, target=START_AT_BLOCK)

node2 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=boot_node)
time.sleep(3)

try:
    logger.info("Checking node2 status. It should not be running.")
    status = node2.get_status()
    sys.exit("node 2 successfully started while it should fail")
except requests.exceptions.ConnectionError:
    pass

logger.info("Checking node2 exit reason.")
node2_correct_exit_reason = False
node2_stderr_path = pathlib.Path(node2.node_dir) / 'stderr'
with open(node2_stderr_path) as stderr_file:
    for line in stderr_file:
        if "cannot sync to the first epoch after sharding upgrade" in line:
            logger.info("Found the correct exit reason in node2 stderr.")
            node2_correct_exit_reason = True
            break

assert node2_correct_exit_reason

logger.info("Test finished.")
