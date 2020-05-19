# The test launches two validating node and two observers
# The first observer tracks no shards, the second observer tracks all shards
# The second observer is used to query balances
# We then send one transaction synchronously through the first observer, and expect it to pass and apply due to rpc tx forwarding

import sys, time, base58, random

sys.path.append('lib')

from cluster import start_cluster
from utils import TxContext
from transaction import sign_payment_tx

nodes = start_cluster(2, 2, 4, None,
                      [["min_gas_price", 0], ["epoch_length", 10],
                       ["block_producer_kickout_threshold", 70]],
                      {3: {
                          "tracked_shards": [0, 1, 2, 3]
                      }})

time.sleep(3)
started = time.time()

old_balances = [
    int(nodes[-1].get_account("test%s" % x)['result']['amount'])
    for x in [0, 1, 2]
]
print("BALANCES BEFORE", old_balances)

status = nodes[1].get_status()
hash_ = status['sync_info']['latest_block_hash']

time.sleep(5)

tx = sign_payment_tx(nodes[0].signer_key, 'test1', 100, 1,
                     base58.b58decode(hash_.encode('utf8')))
print(nodes[-2].send_tx_and_wait(tx, timeout=20))

new_balances = [
    int(nodes[-1].get_account("test%s" % x)['result']['amount'])
    for x in [0, 1, 2]
]
print("BALANCES AFTER", new_balances)

old_balances[0] -= 100
old_balances[1] += 100
assert old_balances == new_balances
