# Spins up a node, then waits for couple epochs
# and spins up another node
# Makes sure that eventually the second node catches up
# Three modes:
#   - notx: no transactions are sent, just checks that
#     the second node starts and catches up
#   - onetx: sends one series of txs at the beginning,
#     makes sure the second node balances reflect them
#   - manytx: constantly issues txs throughout the test
#     makes sure the balances are correct at the end

import sys, time

sys.path.append('lib')

if len(sys.argv) < 3:
    print("python state_sync.py [notx, onetx, manytx] <launch_at_block>")

mode = sys.argv[1]
assert mode in ['notx', 'onetx', 'manytx']

from cluster import init_cluster, spin_up_node, load_config
from utils import TxContext, LogTracker

START_AT_BLOCK = int(sys.argv[2])
TIMEOUT = 150 + START_AT_BLOCK * 10

config = load_config()
near_root, node_dirs = init_cluster(2, 1, 1, config, [["min_gas_price", 0], ["max_inflation_rate", 0], ["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {2: {"tracked_shards": [0]}})

started = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0, None, None)
node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node.node_key.pk, boot_node.addr())
#time.sleep(3)

ctx = TxContext([0, 0], [boot_node, node1])
last_balances = [x for x in ctx.expected_balances]

sent_txs = False

seen_boot_block_indices = set()
observed_block_index = 0
while observed_block_index < START_AT_BLOCK:
    assert time.time() - started < TIMEOUT
    status = boot_node.get_status()
    new_block_index = status['sync_info']['latest_block_index']
    hash_ = status['sync_info']['latest_block_hash']
    if new_block_index > observed_block_index:
        observed_block_index = new_block_index
        seen_boot_block_indices.add(new_block_index)
        print("Boot node got to block_index %s" % new_block_index);

    if mode == 'onetx' and not sent_txs:
        ctx.send_moar_txs(hash_, 3, False)
        sent_txs = True

    elif mode == 'manytx':
        if ctx.get_balances() == ctx.expected_balances:
            ctx.send_moar_txs(hash_, 3, False)
            print("Sending moar txs at block_index %s" % new_block_index)
    time.sleep(0.1)

if mode == 'onetx':
    assert ctx.get_balances() == ctx.expected_balances

node2 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node.node_key.pk, boot_node.addr())
tracker = LogTracker(node2)
time.sleep(3)

catch_up_block_index = 0
while catch_up_block_index < observed_block_index:
    assert time.time() - started < TIMEOUT
    status = node2.get_status()
    new_block_index = status['sync_info']['latest_block_index']
    if new_block_index > catch_up_block_index:
        catch_up_block_index = new_block_index
        print("Second node got to block_index %s" % new_block_index);

    status = boot_node.get_status()
    boot_block_index = status['sync_info']['latest_block_index']
    seen_boot_block_indices.add(boot_block_index)

    if mode == 'manytx':
        if ctx.get_balances() == ctx.expected_balances:
            ctx.send_moar_txs(hash_, 3, False)
            print("Sending moar txs at block_index %s" % boot_block_index)
    time.sleep(0.1)

assert catch_up_block_index in seen_boot_block_indices, "%s not in %s" % (catch_up_block_index, seen_boot_block_indices)

if catch_up_block_index >= 100:
    assert tracker.check("transition to State Sync")
elif catch_up_block_index <= 30:
    assert not tracker.check("transition to State Sync")

if mode == 'manytx':
    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        print("Waiting for the old node to catch up. Current balances: %s; Expected balances: %s" % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)

    # requery the balances from the newly started node
    ctx.nodes.append(node2)
    ctx.act_to_val = [2, 2, 2]

    while ctx.get_balances() != ctx.expected_balances:
        assert time.time() - started < TIMEOUT
        print("Waiting for the new node to catch up. Current balances: %s; Expected balances: %s" % (ctx.get_balances(), ctx.expected_balances))
        time.sleep(1)
