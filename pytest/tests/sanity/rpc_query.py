# The test launches two validating node and two observers
# The first observer tracks all shards, the second observer tracks no shard.
# We send transactions between each pair of accounts through the observer that tracks no shard.
# Check that the transactions succeed and the state at the end is consistent.

import sys, time, base58, random

sys.path.append('lib')

from cluster import start_cluster
from utils import TxContext
from transaction import sign_payment_tx

nodes = start_cluster(2, 2, 4, None,
                      [["min_gas_price", 0], ["epoch_length", 10],
                       ["block_producer_kickout_threshold", 70]],
                      {2: {
                          "tracked_shards": [0, 1, 2, 3]
                      }})

time.sleep(3)

for i in range(4):
    nonce = 1
    status = nodes[0].get_status()
    latest_block_hash = status['sync_info']['latest_block_hash']
    for j in range(4):
        if i != j:
            tx = sign_payment_tx(
                nodes[i].signer_key, 'test%s' % j, 100, nonce,
                base58.b58decode(latest_block_hash.encode('utf8')))
            nonce += 1
            print("sending transaction from test%d to test%d" % (i, j))
            result = nodes[-1].send_tx_and_wait(tx, timeout=25)
            if 'error' in result:
                assert False, result

time.sleep(2)
for i in range(4):

    def fix_result(result):
        result["result"]["block_hash"] = None
        result["result"]["block_height"] = None
        return result

    query_result1 = fix_result(nodes[-2].get_account("test%s" % i))
    query_result2 = fix_result(nodes[-1].get_account("test%s" % i))
    if query_result1 != query_result2:
        print("query same account suspicious %s, %s", query_result1,
              query_result2)
    assert query_result1 == query_result2, "query same account gives different result"
