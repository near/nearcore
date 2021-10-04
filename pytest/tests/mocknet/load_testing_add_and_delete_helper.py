# This file is uploaded to each mocknet node and run there.
# It is responsible for making the node send many transactions
# to itself.

import base58
import json
import random
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
MAX_TPS = 50  # FIXME # maximum transactions per second sent (across the whole network)
# TODO: Get the number of nodes from the genesis config.
# For now this number needs to be in-sync with the actual number of nodes.
NUM_NODES = 100  # FIXME
MAX_TPS_PER_NODE = MAX_TPS / NUM_NODES
# We need to slowly deploy contracts, otherwise we stall out the nodes
CONTRACT_DEPLOY_TIME = 10 * 60
TEST_TIMEOUT = 90 * 60


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
    assert access_keys['result']['keys']
    for k in access_keys['result']['keys']:
        if k['public_key'] == pk:
            return k['access_key']['nonce']


def get_latest_block_hash():
    last_block_hash = get_status()['sync_info']['latest_block_hash']
    return base58.b58decode(last_block_hash.encode('utf8'))


def send_transfer(account, i, node_account_id):
    next_id = (i + 1) % mocknet.NUM_ACCOUNTS
    dest_account_id = mocknet.load_testing_account_id(node_account_id, next_id)
    account.send_transfer_tx(dest_account_id)


function_call_state = {}


def function_call(account, i):
    global function_call_state

    if i not in function_call_state:
        function_call_state[i] = (0, False)

    (cnt, added) = function_call_state[i]
    if added:
        # Note that the f'' strings don't work here, because json needs {} as part of the string.
        s = '{"account_id": "account_' + str(cnt) + '"}'
        logger.info(
            f'Calling function "delete_state" with arguments {s} on account {i}'
        )
        account.send_call_contract_tx('delete_state', bytes(s, encoding='utf8'))
        function_call_state[i] = (cnt + 1, False)
    else:
        s = '{"account_id": "account_' + str(cnt) + '", "message":"' + str(
            cnt) + '"}'
        logger.info(
            f'Calling function "set_state" with arguments {s} on account {i}')
        account.send_call_contract_tx('set_state', bytes(s, encoding='utf8'))
        function_call_state[i] = (cnt, True)


def random_transaction(account, i, node_account_id):
    time.sleep(random.random() * 30)
    choice = random.randint(0, 1)
    if choice == 0:
        logger.info(f'Account {i} transfers')
        send_transfer(account, i, node_account_id)
    elif choice == 1:
        function_call(account, i)


def send_random_transactions(node_account_id, test_accounts):
    pmap(
        lambda account_and_index: random_transaction(account_and_index[
            0], account_and_index[1], node_account_id), test_accounts)


def throttle_txns(send_txns, total_tx_sent, elapsed_time, max_tps,
                  node_account_id, test_accounts):
    start_time = time.time()
    send_txns(node_account_id, test_accounts)
    duration = time.time() - start_time
    total_tx_sent += len(test_accounts)
    elapsed_time += duration

    excess_transactions = total_tx_sent - (max_tps * elapsed_time)
    if excess_transactions > 0:
        delay = excess_transactions / max_tps
        elapsed_time += delay
        logger.info(f'Sleeping for {delay} seconds to throttle transactions')
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
    rpc_nodes = sys.argv[4].split(',')
    logger.info(f'rpc_nodes: {str(rpc_nodes)}')

    test_account_keys = [
        (key.Key(mocknet.load_testing_account_id(node_account_id, i), pk,
                 sk), i) for i in range(mocknet.NUM_ACCOUNTS)
    ]

    base_block_hash = get_latest_block_hash()
    rpc_infos = [(rpc_nodes[i % len(rpc_nodes)], RPC_PORT)
                 for i in range(len(test_account_keys))]

    accounts = [(account.Account(key, get_nonce_for_pk(key.account_id, key.pk),
                                 base_block_hash, rpc_infos[i]), i)
                for (key, i) in test_account_keys]
    return (node_account_id, accounts)


if __name__ == '__main__':
    logger.info(sys.argv)
    (node_account_id, test_accounts) = get_test_accounts_from_args()

    # A random delay to avoid the thundering herd.
    # Sleep before determining `start_time` to prevent different nodes from
    # returning to a resonance.
    time.sleep(random.random()*120)

    start_time = time.time()

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / mocknet.NUM_ACCOUNTS
    logger.info(f'Start deploying, delay between deployments: {delay}')
    for (account, i) in test_accounts:
        logger.info(f'Deploying contract for account {i}')
        account.send_deploy_contract_tx(mocknet.WASM_FILENAME)
        time.sleep(delay)
    logger.info('Done deploying')

    # begin with only transfers for TPS measurement
    total_tx_sent, elapsed_time = 0, 0
    logger.info(
        f'Start the test, expected TPS {MAX_TPS_PER_NODE} over the next {TEST_TIMEOUT} seconds'
    )
    while time.time() - start_time < TEST_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_random_transactions, total_tx_sent,
                                       elapsed_time, MAX_TPS_PER_NODE,
                                       node_account_id, test_accounts)
    logger.info('Stop the test')

    write_tx_events(test_accounts, f'{mocknet.TX_OUT_FILE}.0')
    logger.info('Wrote tx events')
