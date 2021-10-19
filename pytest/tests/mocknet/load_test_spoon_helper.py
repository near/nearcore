# This file is uploaded to each mocknet node and run there.
# It is responsible for making the node send many transactions
# to itself.

import json
import random
import sys
import time

import base58
import requests
from rc import pmap

sys.path.append('lib')
import account
import key
import mocknet
from configured_logger import logger

LOCAL_ADDR = '127.0.0.1'
RPC_PORT = '3030'
# We need to slowly deploy contracts, otherwise we stall out the nodes
CONTRACT_DEPLOY_TIME = 7 * mocknet.NUM_ACCOUNTS
TEST_TIMEOUT = 24 * 60 * 60


def get_status():
    r = requests.get(f'http://{LOCAL_ADDR}:{RPC_PORT}/status', timeout=10)
    r.raise_for_status()
    return json.loads(r.content)


def json_rpc(method, params):
    j = {'method': method, 'params': params, 'id': 'dontcare', 'jsonrpc': '2.0'}
    r = requests.post(f'http://{LOCAL_ADDR}:{RPC_PORT}', json=j, timeout=10)
    return r.json()


def get_nonce_for_pk(account_id, pk, finality='optimistic'):
    access_keys = json_rpc(
        'query', {
            "request_type": "view_access_key_list",
            "account_id": account_id,
            "finality": finality
        })
    assert access_keys['result']['keys'], account_id
    for k in access_keys['result']['keys']:
        if k['public_key'] == pk:
            return k['access_key']['nonce']


def get_latest_block_hash():
    last_block_hash = get_status()['sync_info']['latest_block_hash']
    return base58.b58decode(last_block_hash.encode('utf8'))


def send_transfer(account, i, node_account):
    next_id = random.randint(0, mocknet.NUM_ACCOUNTS - 1)
    dest_account_id = mocknet.load_testing_account_id(
        node_account.key.account_id, next_id)
    account.send_transfer_tx(dest_account_id)


def function_call_set_delete_state(account, i, node_account):
    assert i < len(function_call_state)

    action = None
    if not function_call_state[i]:
        action = "add"
    elif len(function_call_state[i]) >= 100:
        action = "delete"
    else:
        action = random.choice(["add", "delete"])

    if action == "add":
        next_id = random.randint(0, mocknet.NUM_ACCOUNTS - 1)
        next_val = random.randint(0, 1000)
        next_account_id = mocknet.load_testing_account_id(
            node_account.key.account_id, next_id)
        s = '{"account_id": "account_' + str(next_val) + '", "message":"' + str(
            next_val) + '"}'
        logger.info(
            f'Calling function "set_state" of account {next_account_id} with arguments {s} from account {account.key.account_id}'
        )
        tx_res = account.send_call_contract_raw_tx(next_account_id, 'set_state',
                                                   bytes(s, encoding='utf8'), 0)
        logger.info(
            f'{account.key.account_id} set_state on {next_account_id} {tx_res}')
        function_call_state[i].append((next_id, next_val))
    else:
        assert function_call_state[i]
        item = random.choice(function_call_state[i])
        (next_id, next_val) = item
        next_account_id = mocknet.load_testing_account_id(
            node_account.key.account_id, next_id)
        s = '{"account_id": "account_' + str(next_val) + '"}'
        logger.info(
            f'Calling function "delete_state" of account {next_account_id} with arguments {s} from account {account.key.account_id}'
        )
        tx_res = account.send_call_contract_raw_tx(next_account_id,
                                                   'delete_state',
                                                   bytes(s, encoding='utf8'), 0)
        logger.info(
            f'{account.key.account_id} delete_state on {next_account_id} {tx_res}'
        )
        if item in function_call_state[i]:
            function_call_state[i].remove(item)
            logger.info(
                f'Successfully removed {item} from function_call_state. New #items: {len(function_call_state[i])}'
            )
        else:
            logger.info(
                f'{item} is not in function_call_state even though this is impossible. #Items: {len(function_call_state[i])}'
            )


def function_call_ft_transfer_call(account, i, node_account):
    next_id = random.randint(0, mocknet.NUM_ACCOUNTS - 1)
    dest_account_id = mocknet.load_testing_account_id(
        node_account.key.account_id, next_id)

    s = '{"receiver_id": "' + dest_account_id + '", "amount": "3", "msg": "\\"hi\\""}'
    logger.info(
        f'Calling function "ft_transfer_call" with arguments {s} on account {account.key.account_id} contract {dest_account_id}'
    )
    tx_res = account.send_call_contract_raw_tx(dest_account_id,
                                               'ft_transfer_call',
                                               bytes(s, encoding='utf8'), 1)
    logger.info(
        f'{account.key.account_id} ft_transfer to {dest_account_id} {tx_res}')


def random_transaction(account, i, node_account, max_tps_per_node):
    time.sleep(random.random() * mocknet.NUM_ACCOUNTS / max_tps_per_node / 3)
    choice = random.randint(0, 2)
    if choice == 0:
        logger.info(f'Account {i} transfers')
        send_transfer(account, i, node_account)
    elif choice == 1:
        function_call_set_delete_state(account, i, node_account)
    elif choice == 2:
        function_call_ft_transfer_call(account, i, node_account)


def send_random_transactions(node_account, test_accounts, max_tps_per_node):
    pmap(
        lambda account_and_index: random_transaction(account_and_index[
            0], account_and_index[1], node_account, max_tps_per_node),
        test_accounts)


def throttle_txns(send_txns, total_tx_sent, elapsed_time, max_tps_per_node,
                  node_account, test_accounts):
    start_time = time.time()
    send_txns(node_account, test_accounts, max_tps_per_node)
    duration = time.time() - start_time
    total_tx_sent += len(test_accounts)
    elapsed_time += duration

    excess_transactions = total_tx_sent - (max_tps_per_node * elapsed_time)
    if excess_transactions > 0:
        delay = excess_transactions / max_tps_per_node
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
    num_nodes = int(sys.argv[5])
    max_tps = int(sys.argv[6])
    logger.info(f'rpc_nodes: {str(rpc_nodes)}')

    node_account_key = key.Key(node_account_id, pk, sk)
    test_account_keys = [
        (key.Key(mocknet.load_testing_account_id(node_account_id, i), pk,
                 sk), i) for i in range(mocknet.NUM_ACCOUNTS)
    ]

    base_block_hash = get_latest_block_hash()
    rpc_infos = [(rpc_nodes[i % len(rpc_nodes)], RPC_PORT)
                 for i in range(len(test_account_keys))]

    node_account = account.Account(
        node_account_key,
        get_nonce_for_pk(node_account_key.account_id, node_account_key.pk),
        base_block_hash, rpc_infos[0])
    accounts = [(account.Account(key, get_nonce_for_pk(key.account_id, key.pk),
                                 base_block_hash, rpc_infos[i]), i)
                for (key, i) in test_account_keys]
    max_tps_per_node = max_tps / num_nodes
    return (node_account, accounts, max_tps_per_node)


def init_ft(node_account):
    tx_res = node_account.send_deploy_contract_tx(
        '/home/ubuntu/fungible_token.wasm')
    logger.info(f'ft deployment {tx_res}')
    time.sleep(1)

    s = '{"owner_id": "' + node_account.key.account_id + '", "total_supply": "' + str(int(10**33)) + '"}'
    tx_res = node_account.send_call_contract_raw_tx(node_account.key.account_id,
                                                    'new_default_meta',
                                                    bytes(s,
                                                          encoding='utf8'), 0)
    logger.info(f'ft new_default_meta {tx_res}')


def init_ft_account(node_account, account, i):
    s = '{"account_id": "' + account.key.account_id + '"}'
    tx_res = account.send_call_contract_raw_tx(node_account.key.account_id, 'storage_deposit', bytes(s, encoding='utf8'), int(0.00125 * 10**24))
    logger.info(f'Account {account.key.account_id} storage_deposit {tx_res}')

    # The next transaction depends on the previous transaction succeeded.
    # Sleeping for 1 second is the poor man's solution for waiting for that transaction to succeed.
    # This works because the contracts are being deployed slow enough to keep block production above 1 bps.
    time.sleep(1)

    s = '{"receiver_id": "' + account.key.account_id + '", "amount": "' + str(int(10**18)) + '"}'
    logger.info(f'Calling function "ft_transfer" with arguments {s} on account {i}')
    tx_res = node_account.send_call_contract_raw_tx(node_account.key.account_id, 'ft_transfer', bytes(s, encoding='utf8'), 1)
    logger.info(
        f'{node_account.key.account_id} ft_transfer to {account.key.account_id} {tx_res}'
    )


if __name__ == '__main__':
    logger.info(sys.argv)
    (node_account, test_accounts,
     max_tps_per_node) = get_test_accounts_from_args()

    start_time = time.time()

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / mocknet.NUM_ACCOUNTS
    logger.info(f'Start deploying, delay between deployments: {delay}')

    init_ft(node_account)
    for (account, i) in test_accounts:
        logger.info(f'Deploying contract for account {i}')
        account.send_deploy_contract_tx(mocknet.WASM_FILENAME)
        init_ft_account(node_account, account, i)
        time.sleep(delay)

    logger.info('Done deploying')

    global function_call_state
    function_call_state = [[]] * mocknet.NUM_ACCOUNTS

    # begin with only transfers for TPS measurement
    total_tx_sent, elapsed_time = 0, 0
    logger.info(
        f'Start the test, expected TPS {max_tps_per_node} over the next {TEST_TIMEOUT} seconds'
    )
    while time.time() - start_time < TEST_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_random_transactions, total_tx_sent,
                                       elapsed_time, max_tps_per_node,
                                       node_account, test_accounts)
    logger.info('Stop the test')

    write_tx_events(test_accounts, f'{mocknet.TX_OUT_FILE}.0')
    logger.info('Wrote tx events')
