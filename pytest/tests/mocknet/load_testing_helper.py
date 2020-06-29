# This file is uploaded to each mocknet node and run there.
# It is responsible for making the node send many transactions
# to itself.

import base58
import base64
import requests
import json
from rc import pmap
import sys
import time

sys.path.append('lib')
from cluster import Key
from transaction import sign_payment_tx

LOCAL_ADDR = '127.0.0.1'
RPC_PORT = '3030'
NUM_ACCOUNTS = 50
TIMEOUT = 20 * 60  # put under load for 20 minutes


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


def send_transfer(source_account, dest_index):
    alice = source_account
    bob = load_testing_account_id(dest_index)
    alice_nonce = get_nonce_for_pk(alice.account_id, alice.pk)
    last_block_hash = get_status()['sync_info']['latest_block_hash']
    last_block_hash_decoded = base58.b58decode(last_block_hash.encode('utf8'))
    tranfer_amount = 100
    tx = sign_payment_tx(alice, bob, tranfer_amount, alice_nonce + 1,
                         last_block_hash_decoded)
    send_tx(tx)


def send_transfers():
    pmap(
        lambda account_and_index: send_transfer(account_and_index[0], (
            account_and_index[1] + 1) % NUM_ACCOUNTS), test_accounts)


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

    while time.time() - start_time < TIMEOUT:
        send_transfers()
        # TODO: add other transaction types
