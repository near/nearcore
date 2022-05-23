#!/usr/bin/env python3
import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import utils

valid_blocks_only = False  # creating invalid blocks, should be banned instantly
if "valid_blocks_only" in sys.argv:
    valid_blocks_only = True  # creating valid blocks, should be fixed by doom slug

BLOCKS = 25
MALICIOUS_BLOCKS = 50

nodes = start_cluster(
    2, 1, 2, None,
    [["epoch_length", 1000], ["block_producer_kickout_threshold", 80]], {})

started = time.time()

logger.info(f'Waiting for {BLOCKS} blocks...')
height, _ = utils.wait_for_blocks(nodes[1], target=BLOCKS)
logger.info(f'Got to {height} blocks, getting to fun stuff')

nodes[1].get_status(verbose=True)

tracker0 = utils.LogTracker(nodes[0])
res = nodes[1].json_rpc('adv_produce_blocks',
                        [MALICIOUS_BLOCKS, valid_blocks_only])
assert 'result' in res, res
logger.info("Generated %s malicious blocks" % MALICIOUS_BLOCKS)

time.sleep(10)

height = nodes[0].get_latest_block(verbose=True).height

assert height < 40

assert tracker0.check("Banned(BadBlockHeader)")

logger.info("Epic")
