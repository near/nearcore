#!/usr/bin/env python3
"""Spins up a two-node cluster and wait for a few blocks to be produced."""

import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
from configured_logger import logger


def test_sanity_spin_up():
    """Spins up a two-node cluster and wait for a few blocks to be produced.

    This is just a sanity check that the neard binary isn’t borked too much.
    See <https://github.com/near/nearcore/issues/4993>.
    """
    nodes = cluster.start_cluster(2, 0, 1, None, [], {})
    started = time.time()
    while True:
        assert time.time() - started < 10, (
            'Expected three blocks to be generated within 10 seconds.')
        status = nodes[0].get_status()
        block_hash = status['sync_info']['latest_block_hash']
        height = status['sync_info']['latest_block_height']
        logger.info(f'#{height} {block_hash}')
        if height > 3:
            break


if __name__ == '__main__':
    test_sanity_spin_up()
