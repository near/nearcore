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
#import subprocess

sys.path.append('lib')
import account
import key
import mocknet
from configured_logger import logger

LOCAL_ADDR = '127.0.0.1'
RPC_PORT = '3030'
MAX_TPS = 250  # FIXME # maximum transactions per second sent (across the whole network)
# TODO: Get the number of nodes from the genesis config.
# For now this number needs to be in-sync with the actual number of nodes.
NUM_NODES = 100  # FIXME
MAX_TPS_PER_NODE = MAX_TPS / NUM_NODES
# We need to slowly deploy contracts, otherwise we stall out the nodes
CONTRACT_DEPLOY_TIME = 15 * 60
TEST_TIMEOUT = 60 * 60
SKYWARD_INIT_TIME = 120


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
    logger.info(f'get_nonce_for_pk {account_id}')
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



def function_call(account, i, node_account_id):
    if random.randint(0,1) == 0:
        # Note that the f'' strings don't work here, because json needs {} as part of the string.
        s = '{"token_account_id": "token2.near"}'
        logger.info(
            f'Calling function "withdraw_token" with arguments {s} on account {i}'
        )
        tx_res = account.send_call_contract_raw_tx(mocknet.SKYWARD_ACCOUNT, 'withdraw_token', bytes(s, encoding='utf8'), 0)
        logger.info(f'Account {account.key.account_id} withdraw_token {tx_res}')
    else:
        s = '{"sale_id": 0, "amount": "1"}'
        logger.info(
            f'Calling function "sale_deposit_in_token" with arguments {s} on account {i}'
        )
        tx_res = account.send_call_contract_raw_tx(mocknet.SKYWARD_ACCOUNT,
                                          'sale_deposit_in_token',
                                          bytes(s, encoding='utf8'), 1)
        logger.info(f'Account {account.key.account_id} sale_deposit_in_token {tx_res}')


def skyward_transaction(account, i, node_account_id, max_tps):
    time.sleep(random.random() * mocknet.NUM_ACCOUNTS / max_tps / 2)
    function_call(account, i, node_account_id)


def send_skyward_transactions(node_account_id, test_accounts, max_tps):
    pmap(lambda account_and_index: skyward_transaction(account_and_index[0], account_and_index[1], node_account_id, max_tps), test_accounts)


def throttle_txns(send_txns, total_tx_sent, elapsed_time, max_tps,
                  node_account_id, test_accounts):
    start_time = time.time()
    send_txns(node_account_id, test_accounts, max_tps)
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


def get_skyward_account():
    global special_accounts
    return special_accounts[0]


def get_token1_account():
    global special_accounts
    return special_accounts[1]


def get_token2_account():
    global special_accounts
    return special_accounts[2]


def get_account1_account():
    global special_accounts
    return special_accounts[3]

def get_token2_owner_account():
    global special_accounts
    return special_accounts[4]


def initialize_skyward_contract(node_account_id, pk, sk):
    tx_res = get_skyward_account().send_deploy_contract_tx('/home/ubuntu/skyward.wasm')
    logger.info(f'skyward deployment {tx_res}')
    tx_res = get_token1_account().send_deploy_contract_tx(
        '/home/ubuntu/fungible_token.wasm')
    logger.info(f'token1 deployment {tx_res}')
    tx_res = get_token2_account().send_deploy_contract_tx(
        '/home/ubuntu/fungible_token.wasm')
    logger.info(f'token2 deployment {tx_res}')

    time.sleep(2)
    s = '{"skyward_token_id": "' + mocknet.SKYWARD_TOKEN_ACCOUNT + '", "skyward_vesting_schedule":[{"start_timestamp":0,"end_timestamp":1999999999,"amount":"99999999999"}], "listing_fee_near": "10000000000000000000000000", "w_near_token_id":"' + mocknet.MASTER_ACCOUNT + '"}'
    tx_res = get_skyward_account().send_call_contract_tx('new', bytes(s,
                                                             encoding='utf8'))
    logger.info(f'skyward new {tx_res}')

    s = '{"owner_id": "' + mocknet.ACCOUNT1_ACCOUNT + '", "total_supply": "1000000000000000000000000000000000"}'
    tx_res = get_account1_account().send_call_contract_raw_tx(mocknet.TOKEN1_ACCOUNT,
                                                     'new_default_meta',
                                                     bytes(s,
                                                           encoding='utf8'), 0)
    logger.info(f'token1 new_default_meta {tx_res}')

    s = '{"owner_id": "' + mocknet.TOKEN2_OWNER_ACCOUNT + '", "total_supply": "1000000000000000000000000000000000"}'
    tx_res = get_token2_owner_account().send_call_contract_raw_tx(mocknet.TOKEN2_ACCOUNT, 'new_default_meta', bytes(s, encoding='utf8'), 0)
    logger.info(f'token2 new_default_meta {tx_res}')

    time.sleep(2)
    s = '{"token_account_ids": ["' + mocknet.TOKEN1_ACCOUNT + '","' + mocknet.TOKEN2_ACCOUNT + '"]}'
    tx_res = get_account1_account().send_call_contract_raw_tx(mocknet.SKYWARD_ACCOUNT,
                                                     'register_tokens',
                                                     bytes(s, encoding='utf8'),
                                                     0.1 * 1e24)
    logger.info(f'account1 register_tokens {tx_res}')

    s = '{"account_id": "' + mocknet.SKYWARD_ACCOUNT + '"}'
    tx_res = get_account1_account().send_call_contract_raw_tx(mocknet.TOKEN1_ACCOUNT,
                                                     'storage_deposit',
                                                     bytes(s, encoding='utf8'), 1250000000000000000000) # 0.00125 * 1e24)
    logger.info(f'account1 storage_deposit skyward token1 {tx_res}')

    s = '{"account_id": "' + mocknet.SKYWARD_ACCOUNT + '"}'
    tx_res = get_account1_account().send_call_contract_raw_tx(mocknet.TOKEN2_ACCOUNT,
                                                     'storage_deposit',
                                                     bytes(s, encoding='utf8'), 1250000000000000000000) # 0.00125 * 1e24)
    logger.info(f'account1 storage_deposit skyward token2 {tx_res}')

    time.sleep(2)
    s = '{"receiver_id": "' + mocknet.SKYWARD_ACCOUNT + '", "amount": "1000000000000000000000000000000", "memo": "Yolo for sale", "msg": "\\"AccountDeposit\\""}'
    logger.info(f'Calling function "ft_transfer_call" with arguments {s} on account {get_account1_account().key.account_id} contract {mocknet.TOKEN1_ACCOUNT} deposit 1')
    tx_res = get_account1_account().send_call_contract_raw_tx(mocknet.TOKEN1_ACCOUNT,
                                      'ft_transfer_call',
                                      bytes(s, encoding='utf8'), 1)
    logger.info(f'account1 ft_transfer_call to skyward token1 {tx_res}')

    time.sleep(2)
    # Needs to be [7,30] days in the future.
    sale_start_timestamp = round(time.time() + 8 * 24 * 60 * 60)
    s = '{"sale": {"title":"sale","out_tokens":[{"token_account_id":"' + mocknet.TOKEN1_ACCOUNT + '","balance":"500000000000000000000000000000"}], "in_token_account_id": "' + mocknet.TOKEN2_ACCOUNT + '", "start_time": "' + str(sale_start_timestamp) + '000000000", "duration": "3600000000000"}}'
    logger.info( f'Calling function "sale_create" with arguments {s} on account {get_account1_account().key.account_id} for account {mocknet.SKYWARD_ACCOUNT}')
    tx_res = get_account1_account().send_call_contract_raw_tx(mocknet.SKYWARD_ACCOUNT,
                                                     'sale_create',
                                                     bytes(s, encoding='utf8'),
                                                     100 * 1e24)
    logger.info(f'account1 sale_create {tx_res}')
    time.sleep(2)


def get_test_accounts_from_args():
    node_account_id = sys.argv[1]
    pk = sys.argv[2]
    sk = sys.argv[3]
    rpc_nodes = sys.argv[4].split(',')
    if rpc_nodes is None or len(rpc_nodes) == 0 or (len(rpc_nodes) == 1 and not rpc_nodes[0]):
        rpc_nodes = None
    leader_account_id = sys.argv[5]
    upk = sys.argv[6]
    usk = sys.argv[7]

    base_block_hash = get_latest_block_hash()

    test_account_keys = [
        (key.Key(mocknet.load_testing_account_id(node_account_id, i), pk, sk), i) for i in range(mocknet.NUM_ACCOUNTS)
    ]

    rpc_infos = []
    if rpc_nodes:
        rpc_infos = [(rpc_node, RPC_PORT) for rpc_node in rpc_nodes]
    else:
        rpc_infos = [(LOCAL_ADDR, RPC_PORT)]
    logger.info(f'rpc_infos: {rpc_infos}')

    #generateKeyCmd = ["near", "generate-key", "--seedPhrase", f'avocado banana cucumber {node_account_id}']
    #logger.info(f'Generating key {generate}')
    #process = subprocess.Popen(generateKeyCmd, stdout=subprocess.PIPE, env={"NEAR_ENV","local"})
    #output, error = process.communicate()
    #logger.info(f'Key generation output: {output}, error: {error}')

    special_account_keys = [
        key.Key(mocknet.SKYWARD_ACCOUNT, upk, usk),
        key.Key(mocknet.TOKEN1_ACCOUNT, upk, usk),
        key.Key(mocknet.TOKEN2_ACCOUNT, upk, usk),
        key.Key(mocknet.ACCOUNT1_ACCOUNT, upk, usk),
        key.Key(mocknet.TOKEN2_OWNER_ACCOUNT, upk, usk),
    ]
    global special_accounts
    special_accounts = []
    for i in range(len(special_account_keys)):
        kkey = special_account_keys[i]
        special_accounts.append(
            account.Account(special_account_keys[i],
                            get_nonce_for_pk(kkey.account_id, kkey.pk),
                            base_block_hash, rpc_infos[i % len(rpc_infos)]))

    start_time = time.time()
    if node_account_id == leader_account_id:
        initialize_skyward_contract(node_account_id, pk, sk)
        elapsed = time.time() - start_time
        if elapsed < SKYWARD_INIT_TIME:
            logger.info(f'Leader sleeps for {SKYWARD_INIT_TIME-elapsed}sec')
            time.sleep(SKYWARD_INIT_TIME - elapsed)
    else:
        logger.info(f'Non-leader sleeps for {SKYWARD_INIT_TIME}sec')
        time.sleep(SKYWARD_INIT_TIME)

    accounts = []
    for i in range(len(test_account_keys)):
        kkey = test_account_keys[i][0]
        accounts.append((account.Account(kkey, get_nonce_for_pk(kkey.account_id, kkey.pk), base_block_hash, rpc_infos[i%len(rpc_infos)]), i))
    return (node_account_id, accounts)


def init_token2_account(account, i):
    s = '{"account_id": "' + account.key.account_id + '"}'
    tx_res = account.send_call_contract_raw_tx(mocknet.TOKEN2_ACCOUNT, 'storage_deposit', bytes(s, encoding='utf8'), 1250000000000000000000) # 0.00125 * 1e24)
    logger.info(f'Account {account.key.account_id} storage_deposit {tx_res}')

    s = '{"token_account_id": "' + mocknet.TOKEN2_ACCOUNT + '"}'
    logger.info( f'Calling function "register_token" with arguments {s} on account {i}')
    tx_res = account.send_call_contract_raw_tx(mocknet.SKYWARD_ACCOUNT, 'register_token', bytes(s, encoding='utf8'), 0.01 * 1e24)
    logger.info(f'Account {account.key.account_id} register_token token2 {tx_res}')

    time.sleep(1)
    s = '{"receiver_id": "' + account.key.account_id + '", "amount": "1000000000000000000"}'
    logger.info( f'Calling function "ft_transfer" with arguments {s} on account {i}')
    tx_res = get_token2_owner_account().send_call_contract_raw_tx(mocknet.TOKEN2_ACCOUNT, 'ft_transfer', bytes(s, encoding='utf8'), 1)
    logger.info(f'account.token2.near ft_transfer to {account.key.account_id} {tx_res}')


if __name__ == '__main__':
    logger.info(sys.argv)
    (node_account_id, test_accounts) = get_test_accounts_from_args()

    start_time = time.time()

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / mocknet.NUM_ACCOUNTS
    logger.info(f'Start deploying, delay between deployments: {delay}')
    assert delay >= 1
    for (account, i) in test_accounts:
        init_token2_account(account, i)
        time.sleep(delay - 1)
    logger.info('Done deploying')

    # A random delay to avoid the thundering herd.
    # Sleep before determining `start_time` to prevent different nodes from
    # returning to a resonance.
    time.sleep(random.random() * 120)

    # begin with only transfers for TPS measurement
    total_tx_sent, elapsed_time = 0, 0
    logger.info(
        f'Start the test, expected TPS {MAX_TPS_PER_NODE} over the next {TEST_TIMEOUT} seconds'
    )
    while time.time() - start_time < TEST_TIMEOUT:
        (total_tx_sent,
         elapsed_time) = throttle_txns(send_skyward_transactions, total_tx_sent,
                                       elapsed_time, MAX_TPS_PER_NODE,
                                       node_account_id, test_accounts)
    logger.info('Stop the test')

    write_tx_events(test_accounts, f'{mocknet.TX_OUT_FILE}.0')
    logger.info('Wrote tx events')
