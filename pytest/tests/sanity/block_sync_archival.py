#!/usr/bin/env python3
# Spins up one validating node and one non-validating node that is archival. Let the validating node run
# for a while and make sure that the archival node will sync all blocks.

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
import utils

TARGET_HEIGHT = 100
TIMEOUT = 120

client_config0 = {"archive": True}
client_config1 = {
    "archive": True,
    "tracked_shards": [0],
}

nodes = start_cluster(
    1, 1, 1, None,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {
        0: client_config0,
        1: client_config1
    })

nodes[1].kill()

utils.wait_for_blocks(nodes[0], target=TARGET_HEIGHT, timeout=TIMEOUT)

nodes[1].start(boot_node=nodes[1])
time.sleep(2)

utils.wait_for_blocks(nodes[1], target=TARGET_HEIGHT, timeout=TIMEOUT)

for i in range(TARGET_HEIGHT):
    block = nodes[1].json_rpc('block', [i], timeout=15)
    assert 'error' not in block, block
