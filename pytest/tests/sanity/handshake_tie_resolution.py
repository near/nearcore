#!/usr/bin/env python3
"""
Spawn a cluster with four nodes. Check that no node tries to
connect to another node that is currently connected.
"""

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
import utils

BLOCKS = 20

nodes = cluster.start_cluster(4, 0, 4, None, [], {})
trackers = [utils.LogTracker(node) for node in nodes]
utils.wait_for_blocks(nodes[0], target=BLOCKS)
assert all(not tracker.check('Dropping handshake (Active Peer).')
           for tracker in trackers)
