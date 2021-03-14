# This file is uploaded to each mocknet node and run there.
# It is responsible for making the node send many transactions
# to itself.

import base58
import base64
import requests
import json
from rc import pmap, run
import sys
import random
import string
import time

sys.path.append('lib')
from key import Key
from mocknet import NUM_NODES, TX_OUT_FILE
from account import Account

LOCAL_ADDR = '127.0.0.1'
RPC_PORT = '3030'
NUM_ACCOUNTS = 100
MAX_TPS = 500  # maximum transactions per second sent (across the whole network)
MAX_TPS_PER_NODE = MAX_TPS / NUM_NODES
WASM_FILENAME = 'empty_contract_rs.wasm'
# We need to slowly deploy contracts, otherwise we stall out the nodes
CONTRACT_DEPLOY_TIME = 10 * NUM_ACCOUNTS
TRANSFER_ONLY_TIMEOUT = 10 * 60
ALL_TX_TIMEOUT = 10 * 60


def load_testing_account_id(i):
    return f'load_testing_{i}'


def get_status():
    r = requests.get(f'http://{LOCAL_ADDR}:{RPC_PORT}/status', timeout=10)
    r.raise_for_status()
    return json.loads(r.content)


def json_rpc(method, params):
    j = {'method': method, 'params': params, 'id': 'dontcare', 'jsonrpc': '2.0'}
    r = requests.post(f'http://{LOCAL_ADDR}:{RPC_PORT}', json=j, timeout=10)
    return json.loads(r.content)


def get_nonce_for_pk(account_id, pk, finality='optimistic'):
    access_keys = json_rpc(
        'query', {
            "request_type": "view_access_key_list",
            "account_id": account_id,
            "finality": finality
        })
    for k in access_keys['result']['keys']:
        if k['public_key'] == pk:
            return k['access_key']['nonce']


def get_latest_block_hash():
    last_block_hash = get_status()['sync_info']['latest_block_hash']
    return base58.b58decode(last_block_hash.encode('utf8'))


def send_transfer(account, i, i0):
    next_id = i + 1
    if next_id >= i0 + NUM_ACCOUNTS:
        next_id = i0
    dest_account_id = load_testing_account_id(next_id)
    account.send_transfer_tx(dest_account_id)


def random_transaction(account_and_index, i0):
    choice = random.randint(0, 3)
    if choice == 0:
        send_transfer(account_and_index[0], account_and_index[1], i0)
    elif choice == 1:
        account_and_index[0].send_call_contract_tx('do_work', [])
    elif choice == 2:
        new_account_id = ''.join(
            random.choice(string.ascii_lowercase) for _ in range(0, 10))
        account_and_index[0].send_create_account_tx(new_account_id)
    elif choice == 3:
        account_and_index[0].send_stake_tx(1)


def send_transfers(i0):
    pmap(
        lambda account_and_index: send_transfer(account_and_index[
            0], account_and_index[1], i0), test_accounts)


def send_random_transactions(i0):
    pmap(lambda x: random_transaction(x, i0), test_accounts)


def throttle_txns(send_txns, total_tx_sent, elapsed_time, max_tps, i0):
    start_time = time.time()
    send_txns(i0)
    duration = time.time() - start_time
    total_tx_sent += NUM_ACCOUNTS
    elapsed_time += duration

    excess_transactions = total_tx_sent - (max_tps * elapsed_time)
    if excess_transactions > 0:
        delay = excess_transactions / max_tps
        elapsed_time += delay
        time.sleep(delay)

    return (total_tx_sent, elapsed_time)


def write_tx_events(accounts_and_indices):
    # record events for accurate input tps measurements
    all_tx_events = []
    for (account, _) in accounts_and_indices:
        all_tx_events += account.tx_timestamps
    all_tx_events.sort()
    with open(TX_OUT_FILE, 'w') as output:
        for t in all_tx_events:
            output.write(f'{t}\n')


def get_test_accounts_from_args():
    node_index = int(sys.argv[1])
    pk = sys.argv[2]
    sk = sys.argv[3]

    test_account_keys = [
        (Key(load_testing_account_id(i), pk, sk), i)
        for i in range(node_index * NUM_ACCOUNTS, (node_index + 1) *
                       NUM_ACCOUNTS)
    ]

    base_block_hash = get_latest_block_hash()
    rpc_info = (LOCAL_ADDR, RPC_PORT)

    return [(Account(key, get_nonce_for_pk(key.account_id, key.pk),
                     base_block_hash, rpc_info), i)
            for (key, i) in test_account_keys]


if __name__ == '__main__':
    test_accounts = get_test_accounts_from_args()
    run(f'rm -rf {TX_OUT_FILE}')

    i0 = test_accounts[0][1]
    start_time = time.time()

    # begin with only transfers for TPS measurement
    total_tx_sent = 0
    elapsed_time = 0
    while time.time() - start_time < TRANSFER_ONLY_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_transfers, total_tx_sent,
                                       elapsed_time, MAX_TPS_PER_NODE, i0)

    write_tx_events(test_accounts)

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / NUM_ACCOUNTS
    for (account, _) in test_accounts:
        account.send_deploy_contract_tx(WASM_FILENAME)
        time.sleep(delay)

    # reset input transactions
    run(f'rm -rf {TX_OUT_FILE}')
    for (account, _) in test_accounts:
        account.tx_timestamps = []
    # send all sorts of transactions
    start_time = time.time()
    while time.time() - start_time < ALL_TX_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_random_transactions, total_tx_sent,
                                       elapsed_time, MAX_TPS_PER_NODE, i0)

    write_tx_events(test_accounts)
