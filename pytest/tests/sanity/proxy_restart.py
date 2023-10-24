#!/usr/bin/env python3
# Start two nodes. Proxify both nodes. Kill one of them, restart it
# and wait until block at height >= 20.
import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from peer import *
from proxy import ProxyHandler
import utils

TARGET_HEIGHT = 20

if __name__ == '__main__':
    nodes = start_cluster(2, 0, 1, None, [], {}, ProxyHandler)

    nodes[1].kill()
    nodes[1].start(boot_node=nodes[0])

    utils.wait_for_blocks(nodes[1], target=TARGET_HEIGHT)
