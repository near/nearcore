#!/usr/bin/env python3
# This file is uploaded to each mocknet node and run there.
# It is responsible for making the node send many transactions
# to itself.

import json
import itertools
import random
import sys
import time
import pathlib

import base58
import requests
from rc import pmap

# Don't use the pathlib magic because this file runs on a remote machine.
sys.path.append('lib')
import mocknet_helpers
import account
import key
import mocknet
from configured_logger import logger

# We need to slowly deploy contracts, otherwise we stall out the nodes
CONTRACT_DEPLOY_TIME = 12 * mocknet.NUM_ACCOUNTS
TEST_TIMEOUT = 12 * 60 * 60
SKYWARD_INIT_TIME = 120


def send_transfer(account, i, node_account):
    next_id = random.randrange(mocknet.NUM_ACCOUNTS)
    dest_account_id = mocknet.load_testing_account_id(
        node_account.key.account_id, next_id)
    account.send_transfer_tx(dest_account_id)


def function_call(account, i, node_account):
    if random.randint(0, 1) == 0:
        s = f'{{"token_account_id": "token2.near"}}'
        logger.info(
            f'Calling function "withdraw_token" with arguments {s} on account {i}'
        )
        tx_res = mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_call_contract_raw_tx(
                mocknet.SKYWARD_ACCOUNT, 'withdraw_token',
                bytes(s, encoding='utf-8'), 0))
        logger.info(f'Account {account.key.account_id} withdraw_token {tx_res}')
    else:
        s = '{"sale_id": 0, "amount": "1"}'
        logger.info(
            f'Calling function "sale_deposit_in_token" with arguments {s} on account {i}'
        )
        tx_res = mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_call_contract_raw_tx(
                mocknet.SKYWARD_ACCOUNT, 'sale_deposit_in_token',
                bytes(s, encoding='utf-8'), 1))
        logger.info(
            f'Account {account.key.account_id} sale_deposit_in_token {tx_res}')


def skyward_transaction(account, i, node_account, max_tps_per_node):
    time.sleep(random.random() * mocknet.NUM_ACCOUNTS / max_tps_per_node / 3)
    function_call(account, i, node_account)


def send_skyward_transactions(node_account, test_accounts, max_tps_per_node):
    pmap(
        lambda index_and_account: skyward_transaction(index_and_account[
            1], index_and_account[0], node_account, max_tps_per_node),
        enumerate(test_accounts))


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
    return special_accounts[0]


def get_token1_account():
    return special_accounts[1]


def get_token2_account():
    return special_accounts[2]


def get_account1_account():
    return special_accounts[3]


def get_token2_owner_account():
    return special_accounts[4]


def initialize_skyward_contract(node_account_id, pk, sk):
    tx_res = get_skyward_account().send_deploy_contract_tx(
        '/home/ubuntu/skyward.wasm')
    logger.info(f'skyward deployment {tx_res}')
    tx_res = get_token1_account().send_deploy_contract_tx(
        '/home/ubuntu/fungible_token.wasm')
    logger.info(f'token1 deployment {tx_res}')
    tx_res = get_token2_account().send_deploy_contract_tx(
        '/home/ubuntu/fungible_token.wasm')
    logger.info(f'token2 deployment {tx_res}')

    mocknet_helpers.wait_at_least_one_block()
    s = f'{{"skyward_token_id": "{mocknet.SKYWARD_TOKEN_ACCOUNT}", "skyward_vesting_schedule":[{{"start_timestamp":0,"end_timestamp":1999999999,"amount":"99999999999"}}], "listing_fee_near": "10000000000000000000000000", "w_near_token_id":"{mocknet.MASTER_ACCOUNT}"}}'
    tx_res = get_skyward_account().send_call_contract_tx(
        'new', bytes(s, encoding='utf-8'))
    logger.info(f'skyward new {tx_res}')

    s = f'{{"owner_id": "{mocknet.ACCOUNT1_ACCOUNT}", "total_supply": "1000000000000000000000000000000000"}}'
    tx_res = get_account1_account().send_call_contract_raw_tx(
        mocknet.TOKEN1_ACCOUNT, 'new_default_meta', bytes(s, encoding='utf-8'),
        0)
    logger.info(f'token1 new_default_meta {tx_res}')

    s = f'{{"owner_id": "{mocknet.TOKEN2_OWNER_ACCOUNT}", "total_supply": "1000000000000000000000000000000000"}}'
    tx_res = get_token2_owner_account().send_call_contract_raw_tx(
        mocknet.TOKEN2_ACCOUNT, 'new_default_meta', bytes(s, encoding='utf-8'),
        0)
    logger.info(f'token2 new_default_meta {tx_res}')

    mocknet_helpers.wait_at_least_one_block()
    s = f'{{"token_account_ids": ["{mocknet.TOKEN1_ACCOUNT}","' + mocknet.TOKEN2_ACCOUNT + '"]}}'
    tx_res = get_account1_account().send_call_contract_raw_tx(
        mocknet.SKYWARD_ACCOUNT, 'register_tokens', bytes(s, encoding='utf-8'),
        100000000000000000000000)  # int(0.1 * 1e24))
    logger.info(f'account1 register_tokens {tx_res}')

    s = f'{{"account_id": "{mocknet.SKYWARD_ACCOUNT}"}}'
    tx_res = get_account1_account().send_call_contract_raw_tx(
        mocknet.TOKEN1_ACCOUNT, 'storage_deposit', bytes(s, encoding='utf-8'),
        1250000000000000000000)  # 0.00125 * 1e24)
    logger.info(f'account1 storage_deposit skyward token1 {tx_res}')

    s = f'{{"account_id": "{mocknet.SKYWARD_ACCOUNT}"}}'
    tx_res = get_account1_account().send_call_contract_raw_tx(
        mocknet.TOKEN2_ACCOUNT, 'storage_deposit', bytes(s, encoding='utf-8'),
        1250000000000000000000)  # 0.00125 * 1e24)
    logger.info(f'account1 storage_deposit skyward token2 {tx_res}')

    mocknet_helpers.wait_at_least_one_block()
    s = f'{{"receiver_id": "{mocknet.SKYWARD_ACCOUNT}", "amount": "1000000000000000000000000000000", "memo": "Yolo for sale", "msg": "\\"AccountDeposit\\""}}'
    logger.info(
        f'Calling function "ft_transfer_call" with arguments {s} on account {get_account1_account().key.account_id} contract {mocknet.TOKEN1_ACCOUNT} deposit 1'
    )
    tx_res = get_account1_account().send_call_contract_raw_tx(
        mocknet.TOKEN1_ACCOUNT, 'ft_transfer_call', bytes(s, encoding='utf-8'),
        1)
    logger.info(f'account1 ft_transfer_call to skyward token1 {tx_res}')

    mocknet_helpers.wait_at_least_one_block()
    # Needs to be [7,30] days in the future.
    sale_start_timestamp = round(time.monotonic() + 8 * 24 * 60 * 60)
    s = f'{{"sale": {{"title":"sale","out_tokens":[{{"token_account_id":"{mocknet.TOKEN1_ACCOUNT}","balance":"500000000000000000000000000000"}}], "in_token_account_id": "{mocknet.TOKEN2_ACCOUNT}", "start_time": "{str(sale_start_timestamp)}000000000", "duration": "3600000000000"}} }}'
    logger.info(
        f'Calling function "sale_create" with arguments {s} on account {get_account1_account().key.account_id} for account {mocknet.SKYWARD_ACCOUNT}'
    )
    tx_res = get_account1_account().send_call_contract_raw_tx(
        mocknet.SKYWARD_ACCOUNT, 'sale_create', bytes(s, encoding='utf-8'),
        100000000000000000000000000)  # 100 * 1e24
    logger.info(f'account1 sale_create {tx_res}')
    mocknet_helpers.wait_at_least_one_block()


def get_test_accounts_from_args(argv):
    node_account_id = argv[1]
    rpc_nodes = argv[2].split(',')
    logger.info(f'rpc_nodes: {rpc_nodes}')
    num_nodes = int(argv[3])
    max_tps = float(argv[4])
    leader_account_id = sys.argv[5]

    node_account_key = key.Key(node_account_id, mocknet.PUBLIC_KEY,
                               mocknet.SECRET_KEY)
    test_account_keys = [
        key.Key(mocknet.load_testing_account_id(node_account_id, i),
                mocknet.PUBLIC_KEY, mocknet.SECRET_KEY)
        for i in range(mocknet.NUM_ACCOUNTS)
    ]

    base_block_hash = mocknet_helpers.get_latest_block_hash()

    rpc_infos = [(rpc_addr, mocknet_helpers.RPC_PORT) for rpc_addr in rpc_nodes]
    node_account = account.Account(node_account_key,
                                   mocknet_helpers.get_nonce_for_pk(
                                       node_account_key.account_id,
                                       node_account_key.pk),
                                   base_block_hash,
                                   rpc_infos=rpc_infos)
    accounts = [
        account.Account(key,
                        mocknet_helpers.get_nonce_for_pk(
                            key.account_id, key.pk),
                        base_block_hash,
                        rpc_infos=rpc_infos) for key in test_account_keys
    ]
    max_tps_per_node = max_tps / num_nodes

    special_account_keys = [
        key.Key(mocknet.SKYWARD_ACCOUNT, mocknet.PUBLIC_KEY,
                mocknet.SECRET_KEY),
        key.Key(mocknet.TOKEN1_ACCOUNT, mocknet.PUBLIC_KEY, mocknet.SECRET_KEY),
        key.Key(mocknet.TOKEN2_ACCOUNT, mocknet.PUBLIC_KEY, mocknet.SECRET_KEY),
        key.Key(mocknet.ACCOUNT1_ACCOUNT, mocknet.PUBLIC_KEY,
                mocknet.SECRET_KEY),
        key.Key(mocknet.TOKEN2_OWNER_ACCOUNT, mocknet.PUBLIC_KEY,
                mocknet.SECRET_KEY),
    ]
    global special_accounts
    special_accounts = [
        account.Account(key,
                        mocknet_helpers.get_nonce_for_pk(
                            key.account_id, key.pk),
                        base_block_hash,
                        rpc_infos=rpc_infos) for key in special_account_keys
    ]

    start_time = time.monotonic()
    if node_account_id == leader_account_id:
        initialize_skyward_contract(node_account_id, mocknet.PUBLIC_KEY,
                                    mocknet.SECRET_KEY)
        elapsed = time.monotonic() - start_time
        if elapsed < SKYWARD_INIT_TIME:
            logger.info(f'Leader sleeps for {SKYWARD_INIT_TIME-elapsed}sec')
            time.sleep(SKYWARD_INIT_TIME - elapsed)
    else:
        logger.info(f'Non-leader sleeps for {SKYWARD_INIT_TIME}sec')
        time.sleep(SKYWARD_INIT_TIME)

    return node_account, accounts, max_tps_per_node


# This function initializes an account to be used with TOKEN2. Initialization may fail, but this is fine as long as
# most initializations succeed.
def init_token2_account(account, i):
    s = f'{{"account_id": "{account.key.account_id}"}}'
    tx_res = mocknet_helpers.retry_and_ignore_errors(
        lambda: account.send_call_contract_raw_tx(
            mocknet.TOKEN2_ACCOUNT, 'storage_deposit', bytes(s,
                                                             encoding='utf-8'),
            1250000000000000000000))  # 0.00125 * 1e24)
    logger.info(f'Account {account.key.account_id} storage_deposit {tx_res}')

    s = f'{{"token_account_id": "{mocknet.TOKEN2_ACCOUNT}"}}'
    logger.info(
        f'Calling function "register_token" with arguments {s} on account {i}')
    tx_res = mocknet_helpers.retry_and_ignore_errors(
        lambda: account.
        send_call_contract_raw_tx(mocknet.SKYWARD_ACCOUNT, 'register_token',
                                  bytes(s, encoding='utf-8'), 0.01 * 1e24))
    logger.info(
        f'Account {account.key.account_id} register_token token2 {tx_res}')

    # The next transaction depends on the previous transaction succeeded.
    # Sleeping for 1 second is the poor man's solution for waiting for that transaction to succeed.
    # This works because the contracts are being deployed slow enough to keep block production above 1 bps.
    mocknet_helpers.wait_at_least_one_block()

    s = f'{{"receiver_id": "{account.key.account_id}", "amount": "1000000000000000000"}}'
    logger.info(
        f'Calling function "ft_transfer" with arguments {s} on account {i}')
    tx_res = mocknet_helpers.retry_and_ignore_errors(
        get_token2_owner_account().send_call_contract_raw_tx(
            mocknet.TOKEN2_ACCOUNT, 'ft_transfer', bytes(s, encoding='utf-8'),
            1))
    logger.info(
        f'{get_token2_owner_account().key.account_id} ft_transfer to {account.key.account_id} {tx_res}'
    )


def main(argv):
    logger.info(argv)
    (node_account, test_accounts,
     max_tps_per_node) = get_test_accounts_from_args(argv)

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / mocknet.NUM_ACCOUNTS
    logger.info(f'Start deploying, delay between deployments: {delay}')

    start_time = time.monotonic()
    assert delay >= 1
    for i, account in enumerate(test_accounts):
        logger.info(f'Deploying contract for account {i}')
        # Given that large mocknet tests deploy thousands of contracts, some errors are inevitable, ignore them.
        mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_deploy_contract_tx(mocknet.WASM_FILENAME))
        init_token2_account(account, i)
        time.sleep(max(1.0, start_time + (i + 1) * delay - time.monotonic()))
    logger.info('Done deploying')

    # begin with only transfers for TPS measurement
    total_tx_sent, elapsed_time = 0, 0
    logger.info(
        f'Start the test, expected TPS {max_tps_per_node} over the next {TEST_TIMEOUT} seconds'
    )
    last_staking = 0
    start_time = time.monotonic()
    while time.monotonic() - start_time < TEST_TIMEOUT:
        # Repeat the staking transactions in case the validator selection algorithm changes.
        staked_time = mocknet.stake_available_amount(node_account, last_staking)
        if staked_time is not None:
            last_staking = staked_time
        elapsed_time = time.monotonic() - start_time
        total_tx_sent = mocknet_helpers.throttle_txns(
            send_skyward_transactions, total_tx_sent, elapsed_time,
            2 * max_tps_per_node, node_account, test_accounts)
    logger.info('Stop the test')

    write_tx_events(test_accounts, f'{mocknet.TX_OUT_FILE}.0')
    logger.info('Wrote tx events')


if __name__ == '__main__':
    main(sys.argv)
