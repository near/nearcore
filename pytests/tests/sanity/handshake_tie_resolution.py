"""
Spawn a cluster with four nodes. Check that no node tries to
connect to another node that is currently connected.
"""

import sys, time

sys.path.append('lib')

from cluster import start_cluster
from utils import LogTracker

TIMEOUT = 150
BLOCKS = 20

nodes = start_cluster(4, 0, 4, None, [], {})

started = time.time()

trackers = [LogTracker(node) for node in nodes]

while True:
    assert time.time() - started < TIMEOUT

    status = nodes[0].get_status()
    height = status['sync_info']['latest_block_height']
    if height >= BLOCKS:
        break

assert all(not tracker.check('Dropping handshake (Active Peer).')
           for tracker in trackers)
