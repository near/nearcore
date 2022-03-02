#!/usr/bin/env python3
"""Tests that archival node can sync up history from another archival node.

Initialises a cluster with two archival nodes: one validator and one observer.
Starts the validator and waits until several epochs worth of blocks are
generated.  Then, starts the observer node and makes sure that it properly
synchronises the full history.
"""

import pathlib
import sys
import typing

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
from configured_logger import logger
import utils

EPOCH_LENGTH = 5
TARGET_HEIGHT = 20 * EPOCH_LENGTH


class Cluster:

    def __init__(self):
        node_config = {
            'archive': True,
            'tracked_shards': [0],
        }
        self._config = cluster.load_config()
        self._near_root, self._node_dirs = cluster.init_cluster(
            1, 1, 1, self._config, [['epoch_length', EPOCH_LENGTH],
                                    ['block_producer_kickout_threshold', 80]], {
                                        0: node_config,
                                        1: node_config,
                                    })
        self._nodes = [None] * 2

    def start_node(self, ordinal: int) -> cluster.BaseNode:
        assert self._nodes[ordinal] is None
        self._nodes[ordinal] = node = cluster.spin_up_node(
            self._config,
            self._near_root,
            self._node_dirs[ordinal],
            ordinal,
            boot_node=self._nodes[0],
            single_node=not ordinal)
        return node

    def __enter__(self):
        return self

    def __exit__(self, *_):
        for node in self._nodes:
            if node:
                node.cleanup()


def get_all_blocks(node: cluster.BaseNode, *,
                   head: cluster.BlockId) -> typing.Sequence[cluster.BlockId]:
    """Returns all blocks from given head down to genesis block."""
    ids = []
    block_hash = head.hash
    while block_hash != '11111111111111111111111111111111':
        block = node.get_block(block_hash)
        assert 'result' in block, block
        header = block['result']['header']
        ids.append(cluster.BlockId.from_header(header))
        block_hash = header.get('prev_hash')
    return list(reversed(ids))


with Cluster() as nodes:
    # Start the validator and wait for a few epoch’s worth of blocks to be
    # generated.
    boot = nodes.start_node(0)
    latest = utils.wait_for_blocks(boot, target=TARGET_HEIGHT)

    # Start the observer node and wait for it to catch up with the chain state.
    fred = nodes.start_node(1)
    utils.wait_for_blocks(fred, target=TARGET_HEIGHT)

    # Verify that observer got all the blocks.  Note that get_all_blocks
    # verifies that the node has full chain from head to genesis block.
    boot_blocks = get_all_blocks(boot, head=latest)
    fred_blocks = get_all_blocks(fred, head=latest)
    if boot_blocks != fred_blocks:
        for a, b in zip(boot_blocks, fred_blocks):
            if a != b:
                logger.error(f'{a} != {b}')
        assert False
