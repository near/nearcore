# Spins up four nodes, and waits until they produce 100 blocks.
# Ensures that the nodes remained in sync throughout the process
# Sets epoch length to 10

import sys, time, base58

sys.path.append('lib')


from cluster import start_cluster
from transaction import sign_payment_tx

TIMEOUT = 20

nodes = start_cluster(4, 0, 4, {'local': True, 'near_root': '../target/debug/'}, [["epoch_length", 10]])

started = time.time()

initial_balance_0 = int(nodes[3].get_account("test0")['result']['amount'])
initial_balance_1 = int(nodes[2].get_account("test1")['result']['amount'])

sent_tx = False
sent_height = -1

while True:
    assert time.time() - started < TIMEOUT
    status = nodes[3].get_status()

    height = status['sync_info']['latest_block_height']
    hash_ = status['sync_info']['latest_block_hash']

    if not sent_tx:
        if height >= 1:
            tx = sign_payment_tx(nodes[0].account, 'test1', 100, 1, base58.b58decode(hash_.encode('utf8')))
            sent_tx = True
            print(nodes[3].send_tx(tx))
            sent_height = height
            print('Sent tx at height %s' % height)

    else:
        if height == sent_height + 4:
            cur_balance_0 = int(nodes[3].get_account("test0")['result']['amount'])
            cur_balance_1 = int(nodes[2].get_account("test1")['result']['amount'])

            assert cur_balance_0 == initial_balance_0 - 100, "%s != %s - 100" % (cur_balance_0, initial_balance_0)
            assert cur_balance_1 == initial_balance_1 + 100, "%s != %s + 100" % (cur_balance_1, initial_balance_1)
            break

