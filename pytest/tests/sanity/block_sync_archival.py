#!/usr/bin/env python3
"""Tests that archival node can sync up history from another archival node.

The overview of this test is that it starts archival nodes which need to sync
their state from already running archival nodes.  The test can be divided into
two stages:

1. The test first starts a validator and an observer node (let’s call it Fred).
   Both configured as archival nodes.  It then waits for several epochs worth of
   blocks to be generated and received by the observer node.  Once that happens,
   the test kills the validator node so that no new blocks are generated.

   At this stage, the test verifies that Fred can sync correctly and that the
   boot node serves all partial chunks requests from its in-memory cache (which
   is determined by looking at Prometheus metrics).

2. The test then restarts Fred so that its in-memory cache is cleared.  It
   finally starts a new observer (let’s call it Barney) and points it at Fred as
   a boot node.  The test waits for Barney to synchronise with Fred and then
   verifies that all the blocks have been correctly fetched.

   At this stage, the test verifies that Barney synchronises correctly and that
   Fred serves all requests from storage (since it's in-memory cache has been
   cleared).  This is again done through Prometheus metrics and in addition the
   test verifies that data from DBCol::Chunks and DBCol::PartialChunks was used.  This
   also implies that Fred correctly performed DBCol::PartialChunks garbage
   collection.
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

EPOCH_LENGTH = 20
TARGET_HEIGHT = 20 * EPOCH_LENGTH

_DurationMaybe = typing.Optional[datetime.timedelta]


class Cluster:

    def __init__(self):
        node_config = {
            'archive': True,
            'tracked_shards': [0],
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
                3: node_config
            })
        self._nodes = [None] * len(self._node_dirs)

    def start_node(
            self, ordinal: int, *,
            boot_node: typing.Optional[cluster.BaseNode]) -> cluster.BaseNode:
        assert self._nodes[ordinal] is None
        self._nodes[ordinal] = node = cluster.spin_up_node(
            self._config,
            self._near_root,
            self._node_dirs[ordinal],
            ordinal,
            boot_node=boot_node,
            single_node=not ordinal)
        return node

    def __enter__(self):
        return self

    def __exit__(self, *_):
        for node in self._nodes:
            if node:
                node.cleanup()


# TODO(#6458): Move this to separate file and merge with metrics module.
def get_metrics(node_name: str,
                node: cluster.BootNode) -> typing.Dict[str, int]:
    """Fetches partial encoded chunk request count metrics from node.

    Args:
        node_name: Node’s name used when logging the counters.  This is purely
            for debugging.
        node: Node to fetch metrics from.

    Returns:
        A `{key: count}` dictionary where key is in ‘method/success’ format.
        The values correspond to the
        near_partial_encoded_chunk_request_processing_time_count Prometheus
        metric.
    """
    url = 'http://{}:{}/metrics'.format(*node.rpc_addr())
    response = requests.get(url)
    response.raise_for_status()

    metric_name = 'near_partial_encoded_chunk_request_processing_time'
    histogram = next(
        (metric
         for metric in prometheus_client.parser.text_string_to_metric_families(
             response.content.decode('utf8'))
         if metric.name == metric_name), None)
    if not histogram:
        return {}

    counts = dict((sample.labels['method'] + '/' + sample.labels['success'],
                   int(sample.value))
                  for sample in histogram.samples
                  if sample.name.endswith('_count'))
    logger.info(f'{node_name} counters: ' + '; '.join(
        f'{key}: {count}' for key, count in sorted(counts.items())))
    return counts


def assert_metrics(metrics: typing.Dict[str, int],
                   allowed_non_zero: typing.Sequence[str]) -> None:
    """Asserts that only given keys are non-zero.

    Args:
        metrics: Metrics as returned by get_metrics() function.
        allowed_non_zero: Keys that are expected to be non-zero in the metrics.
    """
    for key in allowed_non_zero:
        assert metrics.get(key), f'Expected {key} to be non-zero'
    for key, count in metrics.items():
        ok = key in allowed_non_zero or not count
        assert ok, f'Expected {key} to be zero but got {count}'


def get_all_blocks(node: cluster.BaseNode) -> typing.Sequence[cluster.BlockId]:
    """Returns all blocks from given head down to genesis block."""
    ids = []
    block_hash = node.get_latest_block().hash
    while block_hash != '11111111111111111111111111111111':
        block = node.get_block(block_hash)
        assert 'result' in block, block
        header = block['result']['header']
        ids.append(cluster.BlockId.from_header(header))
        block_hash = header.get('prev_hash')
    return list(reversed(ids))


def run_test(cluster: Cluster) -> None:
    # Start the validator and the first observer.  Wait until the observer
    # synchronises a few epoch’s worth of blocks to be generated and then kill
    # validator so no more blocks are generated.
    boot = cluster.start_node(0, boot_node=None)
    fred = cluster.start_node(1, boot_node=boot)
    utils.wait_for_blocks(fred, target=TARGET_HEIGHT, poll_interval=1)
    metrics = get_metrics('boot', boot)
    boot.kill()

    # We didn’t generate enough blocks to fill boot’s in-memory cache which
    # means all Fred’s requests should be served from it.
    assert_metrics(metrics, ('cache/ok',))

    # Restart Fred so that its cache is cleared.  Then start the second
    # observer, Barney, and wait for it to sync up.
    fred_blocks = get_all_blocks(fred)
    fred.kill(gentle=True)
    fred.start()

    barney = cluster.start_node(2, boot_node=fred)
    utils.wait_for_blocks(barney,
                          target=fred_blocks[-1].height,
                          poll_interval=1)
    barney_blocks = get_all_blocks(barney)
    if fred_blocks != barney_blocks:
        for f, b in zip(fred_blocks, barney_blocks):
            if f != b:
                logger.error(f'{f} != {b}')
        assert False

    # Since Fred’s in-memory cache is clear, all Barney’s requests are served
    # from storage.  Since DBCol::PartialChunks is garbage collected, some of the
    # requests are served from DBCol::Chunks.
    assert_metrics(get_metrics('fred', fred), (
        'chunk/ok',
        'partial/ok',
    ))


if __name__ == '__main__':
    with Cluster() as cl:
        run_test(cl)
