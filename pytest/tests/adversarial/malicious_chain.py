#!/usr/bin/env python3
# starts two validators and one extra RPC node, and directs one of the validators
# to produce invalid blocks. Then we check that the other two nodes have banned this peer.

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

# first check that nodes 0 and 2 have two peers each, before we check later that
# they've dropped to just one peer
network_info0 = nodes[0].json_rpc('network_info', {})['result']
network_info2 = nodes[2].json_rpc('network_info', {})['result']
assert network_info0['num_active_peers'] == 2, network_info0['num_active_peers']
assert network_info2['num_active_peers'] == 2, network_info2['num_active_peers']

nodes[1].get_status(verbose=True)

res = nodes[1].json_rpc('adv_produce_blocks',
                        [MALICIOUS_BLOCKS, valid_blocks_only])
assert 'result' in res, res
logger.info("Generated %s malicious blocks" % MALICIOUS_BLOCKS)

time.sleep(10)

height = nodes[0].get_latest_block(verbose=True).height

assert height < 40

network_info0 = nodes[0].json_rpc('network_info', {})['result']
network_info2 = nodes[2].json_rpc('network_info', {})['result']

# Since we have 3 nodes, they should all have started with 2 peers. After the above
# invalid blocks sent by node1, the other two should have banned it, leaving them
# with one active peer

assert network_info0['num_active_peers'] == 1, network_info0['num_active_peers']
assert network_info2['num_active_peers'] == 1, network_info2['num_active_peers']

logger.info("Epic")
