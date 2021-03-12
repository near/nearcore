# Creates a genesis config with two block producers, and kills one right away after
# launch. Makes sure that the other block producer can produce blocks with chunks and
# process transactions. Makes large-ish number of block producers per shard to minimize
# the chance of the second block producer occupying all the seats in one of the shards

import sys, time, base58, random

sys.path.append('lib')

from cluster import start_cluster
from utils import TxContext
from transaction import sign_payment_tx

TIMEOUT = 180

# give more stake to the bootnode so that it can produce the blocks alone
nodes = start_cluster(
    2, 1, 8, None,
    [["num_block_producer_seats", 199],
     ["num_block_producer_seats_per_shard", [24, 25, 25, 25, 25, 25, 25, 25]],
     ["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 60],
     ["chunk_producer_kickout_threshold", 60],
     ["validators", 0, "amount", "110000000000000000000000000000000"],
     [
         "records", 0, "Account", "account", "locked",
         "110000000000000000000000000000000"
     ], ["total_supply", "4060000000000000000000000000000000"]], {})
time.sleep(3)
nodes[1].kill()

started = time.time()

act_to_val = [0, 0, 0]
ctx = TxContext(act_to_val, nodes)

last_balances = [x for x in ctx.expected_balances]

max_height = 0
sent_height = -1
caught_up_times = 0

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[0].get_status()

    height = status['sync_info']['latest_block_height']
    hash_ = status['sync_info']['latest_block_hash']

    if height > max_height:
        print("Got to height", height)
        max_height = height

    if ctx.get_balances() == ctx.expected_balances:
        print("Balances caught up, took %s blocks, moving on" %
              (height - sent_height))
        ctx.send_moar_txs(hash_, 10, use_routing=True)
        sent_height = height
        caught_up_times += 1
    else:
        if height > sent_height + 30:
            assert False, "Balances before: %s\nExpected balances: %s\nCurrent balances: %s\nSent at height: %s\n" % (
                last_balances, ctx.expected_balances, ctx.get_balances(),
                sent_height)
        time.sleep(0.2)

    if caught_up_times == 3:
        break
