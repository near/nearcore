#!/usr/bin/env python3
"""
Tests that the protobuf to borsh transition can be rolled out without
interrupting the network.

The transition spans four releases, see NetworkTransitionConfig:
* 2.14 sends protobuf messages only, and signs structs over protobuf.
* 2.15 prefers borsh messages but still accepts protobuf ones. It signs structs
  over protobuf, but accepts structs signed over borsh as well.
* 2.16 accepts borsh messages only, and signs structs over borsh.
* 2.17 rejects structs signed over protobuf.

The test simulates those releases with connection_encoding and
signed_struct_format overrides on the current binary and performs a rolling
upgrade through all of them, one release at a time. After each node is
restarted with the new config, the cluster must keep producing blocks and every
node must still be connected to every other node.
"""

import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
from configured_logger import logger
import utils

NUM_NODES = 3
BLOCKS = 10
# Number of blocks the cluster must produce after restarting a node. This
# proves that the restarted node caught up and that the mixed cluster keeps
# making progress.
BLOCKS_AFTER_RESTART = 2

# (connection_encoding, signed_struct_format) of every release.
RELEASE_2_14 = ('ProtobufOnly', 'ProtobufOnly')
RELEASE_2_15 = ('PreferBorsh', 'AcceptBorsh')
RELEASE_2_16 = ('BorshOnly', 'SendBorsh')
RELEASE_2_17 = ('BorshOnly', 'BorshOnly')


def network_config(release):
    connection_encoding, signed_struct_format = release
    return {
        'network': {
            'experimental': {
                'network_config_overrides': {
                    'connection_encoding': connection_encoding,
                    'signed_struct_format': signed_struct_format
                }
            }
        }
    }


def num_peers(node):
    return node.json_rpc('network_info', {})['result']['num_active_peers']


def wait_for_all_peers(nodes):
    """Waits until every node is connected to every other node."""

    def check():
        for i, node in enumerate(nodes):
            peers = num_peers(node)
            assert peers >= NUM_NODES - 1, (
                f'node {i} has {peers} peers, '
                f'expected at least {NUM_NODES - 1}')

    return cluster.nretry(check, timeout=60)


def wait_for_all_blocks(nodes, target):
    for node in nodes:
        utils.wait_for_blocks(node, target=target)


def upgrade(nodes, release):
    """Restarts every node one by one with the config of the given release.

    The rest of the cluster keeps running the previous release, so this
    exercises compatibility between the two adjacent releases.
    """
    for i, node in enumerate(nodes):
        # A node which stays up acts as the boot node for the restarted node.
        boot_node = nodes[(i + 1) % len(nodes)]
        target = max(
            n.get_latest_block().height for n in nodes) + BLOCKS_AFTER_RESTART

        logger.info(f'Restarting node {i} with config {release}')
        node.kill()
        node.change_config(network_config(release))
        node.start(boot_node=boot_node)

        # The restarted node has to catch up, and the cluster has to continue
        # producing blocks despite the mixed releases.
        utils.wait_for_blocks(node, target=target)
        wait_for_all_blocks(nodes, target)
        wait_for_all_peers(nodes)
        logger.info(f'Node {i} is running with config {release}')


def main():
    nodes = cluster.start_cluster(
        NUM_NODES, 0, NUM_NODES, None, [],
        {i: network_config(RELEASE_2_14) for i in range(NUM_NODES)})
    utils.wait_for_blocks(nodes[0], target=BLOCKS)
    wait_for_all_blocks(nodes, target=BLOCKS)
    wait_for_all_peers(nodes)
    logger.info('All nodes are running 2.14 (protobuf only)')

    # 2.14 -> 2.15
    upgrade(nodes, RELEASE_2_15)
    logger.info('All nodes are running 2.15 (borsh preferred)')

    # 2.15 -> 2.16
    upgrade(nodes, RELEASE_2_16)
    logger.info('All nodes are running 2.16 (borsh only, borsh-signed)')

    # 2.16 -> 2.17
    upgrade(nodes, RELEASE_2_17)
    logger.info('All nodes are running 2.17 (protobuf-signed rejected)')

    # The cluster must still be healthy after the whole transition.
    target = utils.wait_for_blocks(nodes[0], count=BLOCKS).height
    wait_for_all_blocks(nodes, target)
    wait_for_all_peers(nodes)


if __name__ == '__main__':
    main()
