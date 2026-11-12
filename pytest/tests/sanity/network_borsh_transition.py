#!/usr/bin/env python3
"""
Tests that the protobuf to borsh transition can be rolled out without
interrupting the network.

The transition spans three releases: 2.14 speaks protobuf only, 2.15 prefers
borsh but still accepts protobuf, and 2.16 accepts borsh only. The test
simulates those releases with connection_encoding overrides on the current
binary and performs a rolling upgrade: first all nodes from 2.14 to 2.15, then
all nodes from 2.15 to 2.16. After each node is restarted with the new config,
the cluster must keep producing blocks and every node must still be connected
to every other node.
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


def network_config(connection_encoding):
    return {
        'network': {
            'experimental': {
                'network_config_overrides': {
                    'connection_encoding': connection_encoding
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


def upgrade(nodes, connection_encoding):
    """Restarts every node one by one with the given connection encoding.

    The rest of the cluster keeps running with the previous encoding, so this
    exercises compatibility between the two adjacent releases.
    """
    for i, node in enumerate(nodes):
        # A node which stays up acts as the boot node for the restarted node.
        boot_node = nodes[(i + 1) % len(nodes)]
        target = max(
            n.get_latest_block().height for n in nodes) + BLOCKS_AFTER_RESTART

        logger.info(f'Restarting node {i} with connection encoding '
                    f'{connection_encoding}')
        node.kill()
        node.change_config(network_config(connection_encoding))
        node.start(boot_node=boot_node)

        # The restarted node has to catch up, and the cluster has to continue
        # producing blocks despite the mixed encodings.
        utils.wait_for_blocks(node, target=target)
        wait_for_all_blocks(nodes, target)
        wait_for_all_peers(nodes)
        logger.info(f'Node {i} is running with connection encoding '
                    f'{connection_encoding}')


def main():
    nodes = cluster.start_cluster(
        NUM_NODES, 0, NUM_NODES, None, [],
        {i: network_config('ProtobufOnly') for i in range(NUM_NODES)})
    utils.wait_for_blocks(nodes[0], target=BLOCKS)
    wait_for_all_blocks(nodes, target=BLOCKS)
    wait_for_all_peers(nodes)
    logger.info('All nodes are running 2.14 (protobuf only)')

    # 2.14 -> 2.15
    upgrade(nodes, 'PreferBorsh')
    logger.info('All nodes are running 2.15 (borsh preferred)')

    # 2.15 -> 2.16
    upgrade(nodes, 'BorshOnly')
    logger.info('All nodes are running 2.16 (borsh only)')

    # The cluster must still be healthy after the whole transition.
    target = utils.wait_for_blocks(nodes[0], count=BLOCKS).height
    wait_for_all_blocks(nodes, target)
    wait_for_all_peers(nodes)


if __name__ == '__main__':
    main()
