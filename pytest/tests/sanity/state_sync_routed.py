#!/usr/bin/env python3
# Spins two block producers and two observers.
# Wait several epochs and spin up another observer that
# is blacklisted by both block producers.
#
# Make sure the new observer sync via routing through other observers.
#
# cspell:words notx manytx onetx
# Three modes:
#   - notx: no transactions are sent, just checks that
#     the second node starts and catches up
#   - onetx: sends one series of txs at the beginning,
#     makes sure the second node balances reflect them
#   - manytx: constantly issues txs throughout the test
#     makes sure the balances are correct at the end

import pathlib
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger
from cluster import init_cluster, spin_up_node, load_config
import state_sync_lib
import utils

if len(sys.argv) < 3:
    logger.info("python state_sync.py [notx, onetx, manytx] <launch_at_block>")
    exit(1)

mode = sys.argv[1]
assert mode in ['notx', 'onetx', 'manytx']

START_AT_BLOCK = int(sys.argv[2])
TIMEOUT = 100 + START_AT_BLOCK * 2

config = load_config()
node_config = state_sync_lib.get_state_sync_config_combined()

near_root, node_dirs = init_cluster(
    2, 3, 1, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 80]],
    {x: node_config for x in range(5)})

started = time.time()

# First observer
node2 = spin_up_node(config, near_root, node_dirs[2], 2)
# Boot from observer since block producer will blacklist third observer
boot_node = node2

# Second observer
node3 = spin_up_node(config, near_root, node_dirs[3], 3, boot_node=boot_node)

# Spin up validators
node0 = spin_up_node(config,
                     near_root,
                     node_dirs[0],
                     0,
                     boot_node=boot_node,
                     blacklist=[4])
node1 = spin_up_node(config,
                     near_root,
                     node_dirs[1],
                     1,
                     boot_node=boot_node,
                     blacklist=[4])

ctx = utils.TxContext([0, 0], [node0, node1])

sent_txs = 0
observed_height = 0
for observed_height, hash_ in utils.poll_blocks(boot_node, timeout=TIMEOUT):
    if observed_height >= START_AT_BLOCK:
        break

    if mode == 'onetx' and sent_txs == 0:
        ctx.send_moar_txs(hash_, 3, False)
        sent_txs += 1

    if mode == 'manytx' and ctx.get_balances() == ctx.expected_balances:
        logger.info(f'Sending moar warm up txs at height {observed_height}')
        ctx.send_moar_txs(hash_, 3, False)
        sent_txs += 1

# Wait for a few blocks to make sure all transactions are processed.
utils.wait_for_blocks(boot_node, count=5)

assert ctx.get_balances() == ctx.expected_balances, "The balances are incorrect"

if mode == 'onetx':
    assert sent_txs == 1, "No transactions were sent"

if mode == 'manytx':
    assert sent_txs > 1, "No transactions were sent"

logger.info("Warm up finished")

node4 = spin_up_node(config,
                     near_root,
                     node_dirs[4],
                     4,
                     boot_node=boot_node,
                     blacklist=[0, 1])

# State Sync makes the storage seem inconsistent.
node4.stop_checking_store()

metrics4 = utils.MetricsTracker(node4)
time.sleep(3)

for block_height, _ in utils.poll_blocks(node4, timeout=TIMEOUT):
    assert time.time() - started < TIMEOUT, "Waiting for node 4 to catch up"

    if mode == 'manytx' and ctx.get_balances() == ctx.expected_balances:
        # Use the boot node head to send more txs. The new node may be behind
        # and its head may already be garbage collected on the boot node. This
        # would cause the transaction to be rejected and the balance check to fail.
        (boot_height, boot_hash) = boot_node.get_latest_block()
        ctx.send_moar_txs(boot_hash, 3, False)
        logger.info(
            f"Sending moar catch up txs at height {boot_height} hash {boot_hash}"
        )

    if block_height >= observed_height:
        break

    time.sleep(0.1)

# The boot heights are the heights of blocks that the node has in its storage.
# It does not contain any blocks that were garbage collected.
boot_heights = boot_node.get_all_heights()
catch_up_height = node4.get_latest_block().height

assert catch_up_height in boot_heights, "%s not in %s" % (catch_up_height,
                                                          boot_heights)

logger.info("Catch Up finished")

while True:
    timeout_expired = time.time() - started < TIMEOUT
    assert timeout_expired, "Waiting for node 4 to connect to two peers"

    num_connections = 0
    peer_connections = metrics4.get_metric_all_values("near_peer_connections")
    for (conn_type, count) in peer_connections:
        if conn_type['tier'] == 'T2':
            num_connections += count
    if num_connections == 2:
        break
    time.sleep(0.1)

logger.info("New node connected to observers")

if mode == 'manytx':
    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        logger.info(
            "Waiting for the old node to catch up. Current balances: %s; Expected balances: %s"
            % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)
    logger.info("Old node caught up to expected balances: %s" %
                ctx.expected_balances)

    # again query the balances from the newly started node
    ctx.nodes.append(node4)
    ctx.act_to_val = [2, 2, 2]

    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        logger.info(
            "Waiting for the new node to catch up. Current balances: %s; Expected balances: %s"
            % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)
    logger.info("New node caught up to expected balances: %s" %
                ctx.expected_balances)
