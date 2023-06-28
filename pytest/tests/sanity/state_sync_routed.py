#!/usr/bin/env python3
# Spins two block producers and two observers.
# Wait several epochs and spin up another observer that
# is blacklisted by both block producers.
#
# Make sure the new observer sync via routing through other observers.
#
# Three modes:
#   - notx: no transactions are sent, just checks that
#     the second node starts and catches up
#   - onetx: sends one series of txs at the beginning,
#     makes sure the second node balances reflect them
#   - manytx: constantly issues txs throughout the test
#     makes sure the balances are correct at the end

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from configured_logger import logger
from cluster import init_cluster, spin_up_node, load_config
import utils

if len(sys.argv) < 3:
    logger.info("python state_sync.py [notx, onetx, manytx] <launch_at_block>")
    exit(1)

mode = sys.argv[1]
assert mode in ['notx', 'onetx', 'manytx']

START_AT_BLOCK = int(sys.argv[2])
TIMEOUT = 150 + START_AT_BLOCK * 10

config = load_config()
node_config = {
    "tracked_shards": [0],
    "state_sync_enabled": True,
    "store.state_snapshot_enabled": True,
}

near_root, node_dirs = init_cluster(
    2, 3, 1, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 80]], {
         0: node_config,
         1: node_config,
         2: node_config,
         3: node_config,
         4: node_config,
     })

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

sent_txs = False

observed_height = 0
for observed_height, hash_ in utils.poll_blocks(boot_node,
                                                timeout=TIMEOUT,
                                                poll_interval=0.1):
    if observed_height >= START_AT_BLOCK:
        break
    if mode == 'onetx' and not sent_txs:
        ctx.send_moar_txs(hash_, 3, False)
        sent_txs = True
    elif mode == 'manytx' and ctx.get_balances() == ctx.expected_balances:
        ctx.send_moar_txs(hash_, 3, False)
        logger.info(f'Sending moar txs at height {observed_height}')

if mode == 'onetx':
    assert ctx.get_balances() == ctx.expected_balances

node4 = spin_up_node(config,
                     near_root,
                     node_dirs[4],
                     4,
                     boot_node=boot_node,
                     blacklist=[0, 1])

metrics4 = utils.MetricsTracker(node4)
time.sleep(3)

catch_up_height = 0
for catch_up_height, hash_ in utils.poll_blocks(node4,
                                                timeout=TIMEOUT,
                                                poll_interval=0.1):
    if catch_up_height >= observed_height:
        break
    assert time.time() - started < TIMEOUT, "Waiting for node 4 to catch up"
    new_height = node4.get_latest_block().height
    logger.info(f"Latest block at: {new_height}")
    if new_height > catch_up_height:
        catch_up_height = new_height
        logger.info(f"Last observer got to height {new_height}")

    boot_height = boot_node.get_latest_block().height

    if mode == 'manytx':
        if ctx.get_balances() == ctx.expected_balances:
            ctx.send_moar_txs(hash_, 3, False)
            logger.info(f"Sending moar txs at height {boot_height}")
    time.sleep(0.1)

boot_heights = boot_node.get_all_heights()

assert catch_up_height in boot_heights, "%s not in %s" % (catch_up_height,
                                                          boot_heights)

while True:
    assert time.time(
    ) - started < TIMEOUT, "Waiting for node 4 to connect to two peers"
    if metrics4.get_int_metric_value("near_peer_connections_total") == 2:
        break
    time.sleep(0.1)

if mode == 'manytx':
    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        logger.info(
            "Waiting for the old node to catch up. Current balances: %s; Expected balances: %s"
            % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)

    # requery the balances from the newly started node
    ctx.nodes.append(node4)
    ctx.act_to_val = [2, 2, 2]

    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        logger.info(
            "Waiting for the new node to catch up. Current balances: %s; Expected balances: %s"
            % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)
