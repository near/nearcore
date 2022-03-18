#!/usr/bin/env python3
"""Tests that archival node can sync up history from another archival node.

Initialises a cluster with three archival nodes: one validator and two
observers.  Starts the validator with one observer keeping in sync.  Once
several epochs worth of blocks are generated kills the validator (so that no new
blocks are generated) and starts the second observer making sure that it
properly synchronises the full history.

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

import prometheus_client.parser
import requests

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
            num_nodes=1,
            num_observers=2,
            num_shards=1,
            config=self._config,
            genesis_config_changes=[['epoch_length', EPOCH_LENGTH],
                                    ['block_producer_kickout_threshold', 80]],
            client_config_changes={
                0: node_config,
                1: node_config,
                2: node_config,
            })
        self._nodes = [None] * len(self._node_dirs)

    def start_node(self,
                   ordinal: int,
                   *,
                   boot_node_index: int = 0) -> cluster.BaseNode:
        assert self._nodes[ordinal] is None
        self._nodes[ordinal] = node = cluster.spin_up_node(
            self._config,
            self._near_root,
            self._node_dirs[ordinal],
            ordinal,
            boot_node=self._nodes[boot_node_index],
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
        # Start the validator and the first observer.  Wait until the observer
        # synchronises a few epoch’s worth of blocks to be generated and then
        # kill validator so no more blocks are generated.
        boot = cluster.start_node(0)
        fred = cluster.start_node(1)
        utils.wait_for_blocks(fred, target=target_height, poll_interval=1)
        boot.kill()
        latest = fred.get_latest_block()

        # Start the second observer node and wait for it to catch up with the
        # first observer.
        barney = cluster.start_node(2, boot_node_index=1)
        utils.wait_for_blocks(barney, target=latest.height, poll_interval=1)

        # Verify that observer got all the blocks.  Note that get_all_blocks
        # verifies that the node has full chain from head to genesis block.
        fred_blocks = get_all_blocks(fred, head=latest)
        barney_blocks = get_all_blocks(barney, head=latest)
        if barney_blocks != fred_blocks:
            for a, b in zip(fred_blocks, barney_blocks):
                if a != b:
                    logger.error(f'{a} != {b}')
            assert False

        # Get near_partial_encoded_chunk_request_processing_time metric
        response = requests.get('http://{}:{}/metrics'.format(*fred.rpc_addr()))
        response.raise_for_status()
        histogram = next(
            metric for metric in prometheus_client.parser.
            text_string_to_metric_families(response.content.decode('utf8')) if
            metric.name == 'near_partial_encoded_chunk_request_processing_time')
        counts = dict((sample.labels['method'] + '/' + sample.labels['success'],
                       int(sample.value))
                      for sample in histogram.samples
                      if sample.name.endswith('_count'))
        logger.info('Counters: ' + '; '.join(
            f'{key}: {count}' for key, count in sorted(counts.items())))

        # In ‘short’ run (i.e. without --long-run flag) we expect all requests
        # to be served from in-memory cache.  In --long-run we expect chunks to
        # be removed from in-memory cache causing some of the requests to be
        # served from partial chunks.
        if opts.long_run:
            keys = ('cache/ok', 'partial/ok')
        else:
            keys = ('cache/ok',)
        for key in keys:
            counts.setdefault(key, 0)
        for key, count in counts.items():
            if key in keys:
                assert count, f'Expected {key} counter to be non-zero'
            else:
                assert not count, (
                    f'Expected {key} counter to be zero but got {count}')


if __name__ == '__main__':
    main(sys.argv)
