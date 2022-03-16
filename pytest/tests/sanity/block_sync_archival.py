#!/usr/bin/env python3
"""Tests that archival node can sync up history from another archival node.

Initialises a cluster with two archival nodes: one validator and one observer.
Starts the validator and waits until several epochs worth of blocks are
generated.  Then, starts the observer node and makes sure that it properly
synchronises the full history.

When called with --long-run the test will generate enough blocks so that entries
in EncodedChunksCache start being evicted.  That it, it’ll generate more than
HEIGHT_HORIZON blocks defined in chunk_cache.rs.  Since that number is 1024,
this may take a while to run but to help with that the validator will be run
with much shorter min_block_production_delay.
"""

import argparse
import datetime
import pathlib
import sys
import typing

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
from configured_logger import logger
import utils

EPOCH_LENGTH = 5
SHORT_TARGET_HEIGHT = 20 * EPOCH_LENGTH
# This must be greater than HEIGHT_HORIZON in chunk_cache.rs
LONG_TARGET_HEIGHT = 1500

_DurationMaybe = typing.Optional[datetime.timedelta]


class Cluster:

    def __init__(self,
                 *,
                 min_block_production_delay: _DurationMaybe = None,
                 max_block_production_delay: _DurationMaybe = None):
        node_config = {
            'archive': True,
            'archive_gc_partial_chunks': True,
            'tracked_shards': [0],
        }

        for key, duration in (('min_block_production_delay',
                               min_block_production_delay),
                              ('max_block_production_delay',
                               max_block_production_delay)):
            if duration:
                secs, td = divmod(duration, datetime.timedelta(seconds=1))
                node_config.setdefault('consensus', {})[key] = {
                    'secs': secs,
                    'nanos': td.microseconds * 1000,
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


def main(argv: typing.Sequence[str]) -> None:
    parser = argparse.ArgumentParser(description='Run an end-to-end test')
    parser.add_argument('--long-run', action='store_true')
    opts = parser.parse_args(argv[1:])

    target_height = SHORT_TARGET_HEIGHT
    min_delay = None
    max_delay = None
    if opts.long_run:
        min_delay = datetime.timedelta(milliseconds=1)
        max_delay = datetime.timedelta(milliseconds=10)
        target_height = LONG_TARGET_HEIGHT

    with Cluster(min_block_production_delay=min_delay,
                 max_block_production_delay=max_delay) as cluster:
        # Start the validator and wait for a few epoch’s worth of blocks to be
        # generated.
        boot = cluster.start_node(0)
        latest = utils.wait_for_blocks(boot,
                                       target=target_height,
                                       poll_interval=1)

        # Start the observer node and wait for it to catch up with the chain
        # state.
        fred = cluster.start_node(1)
        utils.wait_for_blocks(fred, target=target_height, poll_interval=1)

        # Verify that observer got all the blocks.  Note that get_all_blocks
        # verifies that the node has full chain from head to genesis block.
        boot_blocks = get_all_blocks(boot, head=latest)
        fred_blocks = get_all_blocks(fred, head=latest)
        if boot_blocks != fred_blocks:
            for a, b in zip(boot_blocks, fred_blocks):
                if a != b:
                    logger.error(f'{a} != {b}')
            assert False


if __name__ == '__main__':
    main(sys.argv)
