# Connects to the mocknet, issues a tx that sends 100 yoctonear,
# verifies the balances are updated properly.
# Deploys a single contracts, ensures it can be invoked.

import sys, time, random, base58

sys.path.append('lib')

import mocknet
from transaction import sign_payment_tx, sign_function_call_tx, sign_deploy_contract_tx
from utils import load_binary_file

nodes = mocknet.get_nodes()
accounts = mocknet.accounts_from_nodes(nodes)

print()

# Test balance transfers
initial_balances = [
    int(nodes[0].get_account(account.account_id)['result']['amount'])
    for account in accounts
]
nonces = [
    nodes[0].get_nonce_for_pk(account.account_id, account.pk)
    for account in accounts
]

print("INITIAL BALANCES", initial_balances)
print("NONCES", nonces)

last_block_hash = nodes[0].get_status()['sync_info']['latest_block_hash']
last_block_hash_decoded = base58.b58decode(last_block_hash.encode('utf8'))

tx = sign_payment_tx(accounts[0], accounts[1].account_id, 100, nonces[0] + 1,
                     last_block_hash_decoded)
nodes[0].send_tx_and_wait(tx, timeout=10)

new_balances = [
    int(nodes[0].get_account(account.account_id)['result']['amount'])
    for account in accounts
]

print("NEW BALANCES", new_balances)

assert (new_balances[0] + 100) % 1000 == initial_balances[0] % 1000
assert (initial_balances[1] + 100) % 1000 == new_balances[1] % 1000

# Test contract deployment

tx = sign_deploy_contract_tx(
    accounts[2],
    load_binary_file(
        '../runtime/near-test-contracts/res/test_contract_rs.wasm'),
    nonces[2] + 1, last_block_hash_decoded)
nodes[0].send_tx_and_wait(tx, timeout=20)

tx2 = sign_function_call_tx(accounts[2], accounts[2].account_id,
                            'log_something', [], 100000000000, 100000000000,
                            nonces[2] + 2, last_block_hash_decoded)
res = nodes[1].send_tx_and_wait(tx2, 10)
assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'
