#!/usr/bin/env python3
# Spins up two nodes, and waits until they produce 20 blocks.
# Kills the nodes, restarts them, makes sure they produce 20 more blocks
# Sets epoch length to 10

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger

TIMEOUT = 150
BLOCKS1 = 20
BLOCKS2 = 40

nodes = start_cluster(
    2, 0, 2, None,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

started = time.time()

max_height = 0
last_heights = [0 for _ in nodes]
seen_heights = [set() for _ in nodes]
last_common = [[0 for _ in nodes] for _ in nodes]

height_to_hash = {}


def min_common():
    return min([min(x) for x in last_common])


def heights_report():
    for i, sh in enumerate(seen_heights):
        logger.info("Node %s: %s" % (i, sorted(list(sh))))


first_round = True
while max_height < BLOCKS1:
    assert time.time() - started < TIMEOUT
    for i, node in enumerate(nodes):
        height, hash_ = node.get_latest_block()

        if height > max_height:
            max_height = height
            if height % 10 == 0:
                logger.info("Reached height %s, min common: %s" %
                            (height, min_common()))

        if height not in height_to_hash:
            height_to_hash[height] = hash_
        else:
            assert height_to_hash[
                height] == hash_, "height: %s, h1: %s, h2: %s" % (
                    height, hash_, height_to_hash[height])

        last_heights[i] = height
        seen_heights[i].add(height)
        for j, _ in enumerate(nodes):
            if height in seen_heights[j]:
                last_common[i][j] = height
                last_common[j][i] = height
        if not first_round:
            # Don't check it in the first round - min_common will be 0, as we didn't
            # read the status from all nodes.
            assert min_common() + 2 >= height, heights_report()

    first_round = False

assert min_common() + 2 >= BLOCKS1, heights_report()

for node in nodes:
    node.kill()

nodes[0].start()
nodes[1].start(boot_node=nodes[0])

first_round = True
while max_height < BLOCKS2:
    assert time.time() - started < TIMEOUT
    for i, node in enumerate(nodes):
        height, hash_ = node.get_latest_block()

        if height > max_height:
            max_height = height
            if height % 10 == 0:
                logger.info("Reached height %s, min common: %s" %
                            (height, min_common()))

        if height not in height_to_hash:
            height_to_hash[height] = hash_
        else:
            assert height_to_hash[
                height] == hash_, "height: %s, h1: %s, h2: %s" % (
                    height, hash_, height_to_hash[height])

        last_heights[i] = height
        seen_heights[i].add(height)
        for j, _ in enumerate(nodes):
            if height in seen_heights[j]:
                last_common[i][j] = height
                last_common[j][i] = height

        if not first_round:
            # Don't check it in the first round - min_common will be 0, as we didn't
            # read the status from all nodes.
            assert min_common() + 2 >= height, heights_report()

assert min_common() + 2 >= BLOCKS2, heights_report()
