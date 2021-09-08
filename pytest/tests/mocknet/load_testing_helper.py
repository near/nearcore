# This file is uploaded to each mocknet node and run there.
# It is responsible for making the node send many transactions
# to itself.

import base58
import json
import random
# import base64
import requests
import string
import sys
import time
from rc import pmap

sys.path.append('lib')
import account
import key
import mocknet
from configured_logger import logger

LOCAL_ADDR = '127.0.0.1'
RPC_PORT = '3030'
MAX_TPS = 50  # maximum transactions per second sent (across the whole network)
MAX_TPS_PER_NODE = MAX_TPS / mocknet.NUM_NODES
# We need to slowly deploy contracts, otherwise we stall out the nodes
CONTRACT_DEPLOY_TIME = 10 * mocknet.NUM_ACCOUNTS
TRANSFER_ONLY_TIMEOUT = 10 * 60
ALL_TX_TIMEOUT = TRANSFER_ONLY_TIMEOUT + CONTRACT_DEPLOY_TIME + 10 * 60


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
    assert len(access_keys['result']['keys']) > 0
    for k in access_keys['result']['keys']:
        assert 'public_key' in k
        if k['public_key'] == pk:
            return k['access_key']['nonce']


def get_latest_block_hash():
    last_block_hash = get_status()['sync_info']['latest_block_hash']
    return base58.b58decode(last_block_hash.encode('utf8'))


def send_transfer(account, i, node_account_id):
    next_id = (i + 1) % mocknet.NUM_ACCOUNTS
    dest_account_id = mocknet.load_testing_account_id(node_account_id, next_id)
    account.send_transfer_tx(dest_account_id)


def random_transaction(account_and_index, node_account_id):
    choice = random.randint(0, 3)
    if choice == 0:
        send_transfer(account_and_index[0], account_and_index[1], node_account_id)
    elif choice == 1:
        account_and_index[0].send_call_contract_tx('do_work', [])
    elif choice == 2:
        new_account_id = ''.join(random.choice(string.ascii_lowercase) for _ in range(0, 10))
        account_and_index[0].send_create_account_tx(new_account_id)
    elif choice == 3:
        account_and_index[0].send_stake_tx(1)


def send_transfers(node_account_id, test_accounts):
    pmap(lambda account_and_index: send_transfer(account_and_index[0], account_and_index[1], node_account_id),
         test_accounts)


def send_random_transactions(node_account_id, test_accounts):
    pmap(lambda x: random_transaction(x, node_account_id), test_accounts)


def throttle_txns(send_txns, total_tx_sent, elapsed_time, max_tps, node_account_id, test_accounts):
    start_time = time.time()
    send_txns(node_account_id, test_accounts)
    duration = time.time() - start_time
    total_tx_sent += mocknet.NUM_ACCOUNTS
    elapsed_time += duration

    excess_transactions = total_tx_sent - (max_tps * elapsed_time)
    if excess_transactions > 0:
        delay = excess_transactions / max_tps
        elapsed_time += delay
        time.sleep(delay)

    return (total_tx_sent, elapsed_time)


def write_tx_events(accounts_and_indices, filename):
    # record events for accurate input tps measurements
    all_tx_events = []
    for (account, _) in accounts_and_indices:
        all_tx_events += account.tx_timestamps
    all_tx_events.sort()
    with open(filename, 'w') as output:
        for t in all_tx_events:
            output.write(f'{t}\n')


def get_test_accounts_from_args():
    node_account_id = sys.argv[1]
    pk = sys.argv[2]
    sk = sys.argv[3]

    test_account_keys = [(key.Key(mocknet.load_testing_account_id(node_account_id, i), pk, sk), i) for i in
                         range(mocknet.NUM_ACCOUNTS)]

    base_block_hash = get_latest_block_hash()
    rpc_info = (LOCAL_ADDR, RPC_PORT)

    return (node_account_id,
            [(account.Account(key, get_nonce_for_pk(key.account_id, key.pk), base_block_hash, rpc_info), i) for (key, i)
             in test_account_keys])


if __name__ == '__main__':
    logger.info(sys.argv)
    (node_account_id, test_accounts) = get_test_accounts_from_args()

    start_time = time.time()

    # begin with only transfers for TPS measurement
    total_tx_sent = 0
    elapsed_time = 0
    logger.info('Start transfer only')
    while time.time() - start_time < TRANSFER_ONLY_TIMEOUT:
        (total_tx_sent, elapsed_time) = throttle_txns(send_transfers, total_tx_sent, elapsed_time, MAX_TPS_PER_NODE,
                                                      node_account_id, test_accounts)
    logger.info('Stop transfer only')

    write_tx_events(test_accounts, f'{mocknet.TX_OUT_FILE}.0')
    logger.info('Wrote tx events: transfer only')

    logger.info('Start deploying')
    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / mocknet.NUM_ACCOUNTS
    for (account, _) in test_accounts:
        account.send_deploy_contract_tx(mocknet.WASM_FILENAME)
        time.sleep(delay)
    logger.info('Done deploying')

    # reset input transactions
    for (account, _) in test_accounts:
        account.tx_timestamps = []
    # send all sorts of transactions
    start_time = time.time()
    logger.info('Start random transactions')
    while time.time() - start_time < ALL_TX_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_random_transactions, total_tx_sent,
                                       elapsed_time, MAX_TPS_PER_NODE, node_account_id, test_accounts)
    logger.info('Stop random transactions')

    write_tx_events(test_accounts, f'{mocknet.TX_OUT_FILE}.1')
    logger.info('Wrote tx events: random transactions')
