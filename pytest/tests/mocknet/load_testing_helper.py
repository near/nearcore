# This file is uploaded to each mocknet node and run there.
# It is responsible for making the node send many transactions
# to itself.

import base58
import base64
import requests
import json
from rc import pmap
import sys
import random
import string
import time

sys.path.append('lib')
from cluster import Key
from mocknet import NUM_NODES
from transaction import (
    sign_payment_tx, sign_deploy_contract_tx, sign_function_call_tx,
    sign_create_account_with_full_access_key_and_balance_tx, sign_staking_tx)
import utils

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


def send_tx(signed_tx):
    json_rpc('broadcast_tx_async', [base64.b64encode(signed_tx).decode('utf8')])


def get_latest_block_hash():
    last_block_hash = get_status()['sync_info']['latest_block_hash']
    return base58.b58decode(last_block_hash.encode('utf8'))


def send_transfer(source_account, dest_index):
    alice = source_account
    bob = load_testing_account_id(dest_index)
    alice_nonce = get_nonce_for_pk(alice.account_id, alice.pk)
    last_block_hash = get_latest_block_hash()
    tranfer_amount = 100
    tx = sign_payment_tx(alice, bob, tranfer_amount, alice_nonce + 1,
                         last_block_hash)
    send_tx(tx)


def deploy_contract(source_account):
    last_block_hash = get_latest_block_hash()
    nonce = get_nonce_for_pk(source_account.account_id, source_account.pk)
    wasm_binary = utils.load_binary_file(WASM_FILENAME)
    tx = sign_deploy_contract_tx(source_account, wasm_binary, nonce + 1,
                                 last_block_hash)
    send_tx(tx)


def call_contract(source_account):
    last_block_hash = get_latest_block_hash()
    nonce = get_nonce_for_pk(source_account.account_id, source_account.pk)
    tx = sign_function_call_tx(source_account, source_account.account_id,
                               'do_work', [], 300000000000000, 0, nonce + 1,
                               last_block_hash)
    send_tx(tx)


def create_account(source_account):
    last_block_hash = get_latest_block_hash()
    nonce = get_nonce_for_pk(source_account.account_id, source_account.pk)
    new_account_id = ''.join(
        random.choice(string.ascii_lowercase) for _ in range(0, 10))
    new_key = Key(new_account_id, source_account.pk, source_account.sk)
    tx = sign_create_account_with_full_access_key_and_balance_tx(
        source_account, new_account_id, new_key, 100, nonce + 1,
        last_block_hash)
    send_tx(tx)


def stake(source_account):
    last_block_hash = get_latest_block_hash()
    nonce = get_nonce_for_pk(source_account.account_id, source_account.pk)
    tx = sign_staking_tx(source_account, source_account, 1, nonce + 1,
                         last_block_hash)
    send_tx(tx)


def random_transaction(account_and_index):
    choice = random.randint(0, 3)
    if choice == 0:
        send_transfer(account_and_index[0], account_and_index[1] + 1)
    elif choice == 1:
        call_contract(account_and_index[0])
    elif choice == 2:
        create_account(account_and_index[0])
    elif choice == 3:
        stake(account_and_index[0])


def send_transfers():
    pmap(
        lambda account_and_index: send_transfer(account_and_index[0], (
            account_and_index[1] + 1) % NUM_ACCOUNTS), test_accounts)


def send_random_transactions():
    pmap(random_transaction, test_accounts)


def throttle_txns(send_txns, total_tx_sent, elapsed_time, max_tps):
    start_time = time.time()
    send_txns()
    duration = time.time() - start_time
    total_tx_sent += NUM_ACCOUNTS
    elapsed_time += duration

    excess_transactions = total_tx_sent - (max_tps * elapsed_time)
    if excess_transactions > 0:
        delay = excess_transactions / max_tps
        elapsed_time += delay
        time.sleep(delay)

    return (total_tx_sent, elapsed_time)


if __name__ == '__main__':
    node_index = int(sys.argv[1])
    pk = sys.argv[2]
    sk = sys.argv[3]

    test_accounts = [
        (Key(load_testing_account_id(i), pk, sk), i)
        for i in range(node_index * NUM_ACCOUNTS, (node_index + 1) *
                       NUM_ACCOUNTS)
    ]

    start_time = time.time()

    # begin with only transfers for TPS measurement
    total_tx_sent = 0
    elapsed_time = 0
    while time.time() - start_time < TRANSFER_ONLY_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_transfers, total_tx_sent,
                                       elapsed_time, MAX_TPS_PER_NODE)

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / NUM_ACCOUNTS
    for x in test_accounts:
        deploy_contract(x[0])
        time.sleep(delay)

    # send all sorts of transactions
    start_time = time.time()
    while time.time() - start_time < ALL_TX_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_random_transactions, total_tx_sent,
                                       elapsed_time, MAX_TPS_PER_NODE)
