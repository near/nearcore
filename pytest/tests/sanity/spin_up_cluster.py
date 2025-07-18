#!/usr/bin/env python3
"""Spins up a two-node cluster and wait for a few blocks to be produced."""

import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
from configured_logger import logger
import utils


def test_sanity_spin_up():
    """Spins up a two-node cluster and wait for a few blocks to be produced.

    Sets store.path of one of the node to something other than `data` to test if
    that option works as well.

    This is just a sanity check that the neard binary isn’t borked too much.
    See <https://github.com/near/nearcore/issues/4993>.
    """
    # cspell:ignore atad, ehcac, tcartnoc
    client_config_changes = {
        1: {
            'store': {
                'path': 'atad'
            },
            'contract_cache_path': 'atad/ehcac.tcartnoc'
        }
    }
    nodes = cluster.start_cluster(2,
                                  0,
                                  1,
                                  None, [],
                                  client_config_changes=client_config_changes)
    utils.wait_for_blocks(nodes[0], target=4)
    # Verify that second node created RocksDB in ‘atad’ directory rather than
    # ‘data’.
    assert not (pathlib.Path(nodes[1].node_dir) / 'data').exists()
    store_path = pathlib.Path(
        nodes[1].node_dir) / client_config_changes[1]['store']['path']
    assert store_path.exists()
    assert not (store_path / 'contract.cache').exists()
    assert (pathlib.Path(nodes[1].node_dir) /
            client_config_changes[1]['contract_cache_path']).exists()


if __name__ == '__main__':
    test_sanity_spin_up()
