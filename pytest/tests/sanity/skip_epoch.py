#!/usr/bin/env python3
# Tests a situation when in a given shard has all BPs offline
# Two specific cases:
#  - BPs never showed up to begin with, since genesis
#  - BPs went offline after some epoch
# Warn: this test may not clean up ~/.near if fails early

import sys, time, base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from transaction import sign_staking_tx
import utils

TIMEOUT = 600
# the height we spin up the second node
TARGET_HEIGHT = 35
EPOCH_LENGTH = 12

config = load_config()
# give more stake to the bootnode so that it can produce the blocks alone
near_root, node_dirs = init_cluster(
    4, 1, 4, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]],
     ["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 20],
     ["chunk_producer_kickout_threshold", 20]], {
         0: {
             "view_client_throttle_period": {
                 "secs": 0,
                 "nanos": 0
             },
             "consensus": {
                 "state_sync_timeout": {
                     "secs": 2,
                     "nanos": 0
                 }
             }
         },
         1: {
             "view_client_throttle_period": {
                 "secs": 0,
                 "nanos": 0
             },
             "consensus": {
                 "state_sync_timeout": {
                     "secs": 2,
                     "nanos": 0
                 }
             }
         },
         2: {
             "view_client_throttle_period": {
                 "secs": 0,
                 "nanos": 0
             },
             "consensus": {
                 "state_sync_timeout": {
                     "secs": 2,
                     "nanos": 0
                 }
             }
         },
         3: {
             "view_client_throttle_period": {
                 "secs": 0,
                 "nanos": 0
             },
             "consensus": {
                 "state_sync_timeout": {
                     "secs": 2,
                     "nanos": 0
                 }
             }
         },
         4: {
             "tracked_shards": [0, 1, 2, 3],
             "view_client_throttle_period": {
                 "secs": 0,
                 "nanos": 0
             }
         }
     })

started = time.time()

boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
boot_node.stop_checking_store()
node3 = spin_up_node(config, near_root, node_dirs[2], 2, boot_node=boot_node)
node4 = spin_up_node(config, near_root, node_dirs[3], 3, boot_node=boot_node)
observer = spin_up_node(config, near_root, node_dirs[4], 4, boot_node=boot_node)
observer.stop_checking_store()

ctx = utils.TxContext([4, 4, 4, 4, 4],
                      [boot_node, None, node3, node4, observer])
initial_balances = ctx.get_balances()
total_supply = sum(initial_balances)

logger.info("Initial balances: %s\nTotal supply: %s" %
            (initial_balances, total_supply))

sent_txs = False
largest_height = 0

# 1. Make the first node get to height 35. The second epoch will end around height 24-25,
#    which would already result in a stall if the first node can't sync the state from the
#    observer for the shard it doesn't care about
for height, hash_ in utils.poll_blocks(observer,
                                       timeout=TIMEOUT,
                                       poll_interval=0.1):
    if height >= TARGET_HEIGHT:
        break
    if height > 1 and not sent_txs:
        ctx.send_moar_txs(hash_, 10, False)
        logger.info(f'Sending txs at height {height}')
        sent_txs = True

logger.info("stage 1 done")

# 2. Spin up the second node and make sure it gets to 35 as well, and doesn't diverge
node2 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node=boot_node)
node2.stop_checking_store()

while True:
    assert time.time() - started < TIMEOUT
    sync_info = node2.get_status()['sync_info']
    if (sync_info['latest_block_height'] > TARGET_HEIGHT and
            not sync_info['syncing']):
        break
    time.sleep(0.1)

logger.info("stage 2 done")

# 3. During (1) we sent some txs. Make sure the state changed. We can't compare to the
#    expected balances directly, since the tx sent to the shard that node1 is responsible
#    for was never applied, but we can make sure that some change to the state was done,
#    and that the totals match (= the receipts was received)
#    What we are testing here specifically is that the first node received proper incoming
#    receipts during the state sync from the observer.
#    `max_inflation_rate` is set to zero, so the rewards do not mess up with the balances
balances = ctx.get_balances()
logger.info("New balances: %s\nNew total supply: %s" %
            (balances, sum(balances)))

assert (balances != initial_balances)
assert (sum(balances) == total_supply)

initial_balances = balances

logger.info("stage 3 done")

# 4. Stake for the second node to bring it back up as a validator and wait until it actually
#    becomes one


def get_validators():
    return set([x['account_id'] for x in boot_node.get_status()['validators']])


logger.info(get_validators())

# The stake for node2 must be higher than that of boot_node, so that it can produce blocks
# after the boot_node is brought down
tx = sign_staking_tx(node2.signer_key, node2.validator_key,
                     50000000000000000000000000000000, 20,
                     base58.b58decode(hash_.encode('utf8')))
boot_node.send_tx(tx)

validators = get_validators()
assert validators == set(["test0", "test2", "test3"]), validators

while True:
    if time.time() - started > TIMEOUT:
        logger.info(get_validators())
        assert False

    if get_validators() == set(["test0", "test1", "test2", "test3"]):
        break

    time.sleep(1)

logger.info("stage 4 done")

ctx.next_nonce = 100
# 5. Bring down the first node, then wait until epoch T+3
last_height = observer.get_latest_block().height

ctx.nodes = [boot_node, node2, node3, node4, observer]
ctx.act_to_val = [4, 4, 4, 4, 4]

boot_node.kill()

for height, hash_ in utils.poll_blocks(observer,
                                       timeout=TIMEOUT,
                                       poll_interval=0.1):
    if height > last_height + 1:
        ctx.send_moar_txs(hash_, 10, False)
        logger.info(f'Sending txs at height {height}')
        break

start_epoch = -1
for epoch_height in utils.poll_epochs(observer,
                                      epoch_length=EPOCH_LENGTH,
                                      timeout=TIMEOUT):
    if start_epoch == -1:
        start_epoch = epoch_height
    if epoch_height >= start_epoch + 3:
        break

balances = ctx.get_balances()
logger.info("New balances: %s\nNew total supply: %s" %
            (balances, sum(balances)))

ctx.nodes = [observer, node2]
ctx.act_to_val = [0, 0, 0, 0, 0]
logger.info("Observer sees: %s" % ctx.get_balances())

assert balances != initial_balances, "current balance %s, initial balance %s" % (
    balances, initial_balances)
assert sum(balances) == total_supply
