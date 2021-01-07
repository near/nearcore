# Handling relays automatically.
# Send several txs from eth to near, then from near to eth back.
# Guarantee that all txs will be in same block.

import base58
from retrying import retry
import sys, time

if len(sys.argv) < 3:
    print("python same_block.py <eth2near_tx_number> <near2eth_tx_number> [...]")
    exit(1)

eth2near_tx_number = int(sys.argv[1])
near2eth_tx_number = int(sys.argv[2])
assert eth2near_tx_number >= 0 and eth2near_tx_number <= 1000 or eth2near_tx_number == 0 and near2eth_tx_number == 0
assert near2eth_tx_number >= 0 and near2eth_tx_number <= 1000

sys.path.append('lib')

from cluster import start_cluster, start_bridge
from bridge import alice, bridge_cluster_config_changes, BridgeTx, BridgeTxDirection
from transaction import sign_function_call_tx

@retry(wait_exponential_multiplier=1.2, wait_exponential_max=100)
def send_tx(node, tx):
    res = node.send_tx(tx)
    print(res)
    return res['result']

def near2eth_custom_withdraw(tx, node, adapter):
    return tx.custom['withdraw_ticket']

nodes = start_cluster(2, 0, 1, None, [], bridge_cluster_config_changes)
(bridge, ganache) = start_bridge(nodes)

# TODO do the same for eth txs
txs = []
for _ in range(eth2near_tx_number):
    txs.append(bridge.transfer_eth2near(alice, 1000))
[p.join() for p in txs]
exit_codes = [p.exitcode for p in txs]

assert exit_codes == [0 for _ in txs]
bridge.check_balances(alice)

status = nodes[0].get_status(check_storage=False)
h = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
while True:
    # wait until new block starts
    time.sleep(0.1)
    try:
        status = nodes[0].get_status(check_storage=False)
        hh = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
        if h != hh:
            print(h, status, "new block just appeared!")
            h = hh
            break
    except:
        pass

withdraws = []
base_nonce = nodes[0].get_nonce_for_pk(alice.near_account_name, alice.near_signer_key.pk)
for nonce_increment in range(near2eth_tx_number):
    tx = BridgeTx(BridgeTxDirection.NEAR2ETH, alice, alice, 1)
    receiver_address = tx.receiver.eth_address
    if receiver_address.startswith('0x'):
        receiver_address = receiver_address[2:]
    withdraw_tx = sign_function_call_tx(
        tx.sender.near_signer_key,
        tx.near_token_account_id,
        'withdraw',
        bytes('{"amount": "' + str(tx.amount) + '", "recipient": "' + receiver_address + '"}', encoding='utf8'),
        300000000000000,
        0,
        base_nonce + nonce_increment + 1,
        h)
    withdraws.append((send_tx(nodes[0], withdraw_tx), tx))

status = nodes[0].get_status(check_storage=False)
hh = base58.b58decode(status['sync_info']['latest_block_hash'].encode('utf8'))
if h != hh:
    print(h, status, "not succeded in putting all txs into one block")
    assert False

txs = []
for (w, tx) in withdraws:
    tx.custom['withdraw_ticket'] = w
    txs.append(bridge.transfer_tx(tx, withdraw_func=near2eth_custom_withdraw))
    # sleep to avoid peak load
    time.sleep(3)
[p.join() for p in txs]
exit_codes = [p.exitcode for p in txs]

assert exit_codes == [0 for _ in txs]
bridge.check_balances(alice)


print('EPIC')
