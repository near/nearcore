#!/usr/bin/env python3
"""Tests whether node can continue working after its storage is recompressed.

The test starts a cluster of four nodes, submits a few transactions onto the
network.  The same transactions as the ones used in rpc_tx_status.py test.

Once that’s done, each node is stopped and its storage processed via the
‘recompress-storage’ command.  ‘view-state apply-range’ is then executed to
verify that the database has not been corrupted.

Finally, all the nodes are restarted and again a few transactions are sent to
verify that everything is working in order.

The above steps are done once for RPC nodes and again for archival nodes.
"""

import os
import pathlib
import subprocess
import sys
import threading
import time
import typing
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import key
import transaction

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'tests'))

import sanity.rpc_tx_status


class RecompressStorageTestCase(unittest.TestCase):

    def __init__(self, *args, **kw) -> None:
        super().__init__(*args, **kw)
        self.nodes = ()

    def tearDown(self) -> None:
        for node in self.nodes:
            node.cleanup()
        self.nodes = ()

    def do_test_recompress_storage(self, *, archive: bool) -> None:
        logger.info(f'start cluster')
        self.nodes = sanity.rpc_tx_status.start_cluster(archive=archive)

        # Give the network some time to generate a few blocks.  The same goes
        # for other sleeps in this method.
        time.sleep(5)

        # Execute a few transactions
        sanity.rpc_tx_status.test_tx_status(self.nodes)
        time.sleep(5)

        # Recompress storage on each node
        logger.info(f'Recompress storage on each node')
        for idx, node in enumerate(self.nodes):
            logger.info(f'Stopping node{idx}')
            node.kill(gentle=True)

            node_dir = pathlib.Path(node.node_dir)
            self._call(
                node,
                'recompress-storage-',
                'recompress-storage',
                '--output-dir=' + str(node_dir / 'data-new'),
            )
            (node_dir / 'data').rename(node_dir / 'data-old')
            (node_dir / 'data-new').rename(node_dir / 'data')

            self._call(
                node,
                'view-state-',
                'view-state',
                'apply-range',
                '--start-index=0',
                '--verbose-output',
            )

        # Restart all nodes with the new database
        logger.info(f'Restart all nodes with the new database')
        for idx, node in enumerate(self.nodes):
            logger.info(f'Starting node{idx}')
            node.start(boot_node=self.nodes[0])

        # Execute a few more transactions
        time.sleep(5)
        sanity.rpc_tx_status.test_tx_status(self.nodes, nonce_offset=3)

    def _call(self, node: cluster.LocalNode, prefix: str, *args:
              typing.Union[str, pathlib.Path]) -> None:
        """Calls node’s neard with given arguments."""
        node_dir = pathlib.Path(node.node_dir)
        cmd = [
            pathlib.Path(node.near_root) / node.binary_name,
            f'--home={node_dir}',
        ] + list(args)
        logger.info('Running ' + ' '.join(str(arg) for arg in cmd))
        with open(node_dir / (prefix + 'stdout'), 'ab') as stdout, \
             open(node_dir / (prefix + 'stderr'), 'ab') as stderr:
            subprocess.check_call(cmd,
                                  stdin=subprocess.DEVNULL,
                                  stdout=stdout,
                                  stderr=stderr,
                                  env=dict(os.environ, RUST_LOG='debug'))

    def test_recompress_storage_rpc(self) -> None:
        self.do_test_recompress_storage(archive=False)

    def test_recompress_storage_archive(self) -> None:
        self.do_test_recompress_storage(archive=True)


if __name__ == '__main__':
    unittest.main()
