#!/usr/bin/env python3
# Spin up one validator node and let it run for a while
# Spin up another node that does state sync. Keep sending
# transactions to that node and make sure it doesn't crash.

import sys, time, base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, Key
from configured_logger import logger
import state_sync_lib
from transaction import sign_payment_tx
import utils

MAX_SYNC_WAIT = 30
EPOCH_LENGTH = 20

(node_config_dump,
 node_config_sync) = state_sync_lib.get_state_sync_configs_pair()

nodes = start_cluster(
    1, 1, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 10],
     ["chunk_producer_kickout_threshold", 10]], {
         0: node_config_dump,
         1: node_config_sync,
     })
time.sleep(2)
nodes[1].kill()
logger.info('node1 is killed')

# Grow chain long enough for epoch sync proof derivation (>= 4 * EPOCH_LENGTH).
cur_height, _ = utils.wait_for_blocks(nodes[0], target=EPOCH_LENGTH * 5)

genesis_block = nodes[0].json_rpc('block', [0])
genesis_hash = genesis_block['result']['header']['hash']
genesis_hash = base58.b58decode(genesis_hash.encode('ascii'))

nodes[1].start_with_epoch_sync_restart(boot_node=nodes[1])
tracker = utils.LogTracker(nodes[1])
time.sleep(1)

start_time = time.time()
node1_height = 0
nonce = 1
while node1_height <= cur_height:
    if time.time() - start_time > MAX_SYNC_WAIT * 3:
        assert False, "state sync timed out"
    if nonce % 5 == 0:
        node1_height = nodes[1].get_latest_block(verbose=True).height
    tx = sign_payment_tx(nodes[0].signer_key, 'test1', 1, nonce, genesis_hash)
    nodes[1].send_tx(tx)
    nonce += 1
    time.sleep(0.05)

assert tracker.check('transition to state sync')
