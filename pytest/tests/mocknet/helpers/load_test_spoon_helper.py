#!/usr/bin/env python3
"""
Generates transactions on a mocknet node.
This file is uploaded to each mocknet node and run there.
"""

import random
import sys
import time

# Don't use the pathlib magic because this file runs on a remote machine.
sys.path.append('lib')
import account
import key as key_mod
import load_test_utils
import mocknet
import mocknet_helpers

from configured_logger import logger

# We need to slowly deploy contracts, otherwise we stall out the nodes
CONTRACT_DEPLOY_TIME = 10 * mocknet.NUM_ACCOUNTS
TEST_TIMEOUT = 12 * 60 * 60


def get_test_accounts_from_args(argv):
    node_account_id = argv[1]
    rpc_nodes = argv[2].split(',')
    num_nodes = int(argv[3])
    max_tps = float(argv[4])
    logger.info(f'rpc_nodes: {rpc_nodes}')

    node_account_key = key_mod.Key(node_account_id, mocknet.PUBLIC_KEY,
                                   mocknet.SECRET_KEY)
    test_account_keys = [
        key_mod.Key(mocknet.load_testing_account_id(node_account_id, i),
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
    return load_test_utils.TestState(
        node_account,
        accounts,
        max_tps_per_node,
        rpc_infos,
    )


def main(argv):
    logger.info(argv)
    test_state = get_test_accounts_from_args(argv)

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = CONTRACT_DEPLOY_TIME / test_state.num_test_accounts()
    logger.info(f'Start deploying, delay between deployments: {delay}')

    time.sleep(random.random() * delay)
    start_time = time.monotonic()
    assert delay >= 1
    load_test_utils.init_ft(test_state.node_account)
    for i, account in enumerate(test_state.test_accounts):
        logger.info(f'Deploying contract for account {account.key.account_id}')
        mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_deploy_contract_tx(mocknet.WASM_FILENAME))
        load_test_utils.init_ft_account(test_state.node_account, account)
        logger.info(
            f'Account {account.key.account_id} balance after initialization: {mocknet_helpers.retry_and_ignore_errors(lambda:account.get_amount_yoctonear())}'
        )
        time.sleep(max(1.0, start_time + (i + 1) * delay - time.monotonic()))

    logger.info('Done deploying')

    # begin with only transfers for TPS measurement
    total_tx_sent, elapsed_time = 0, 0
    logger.info(
        f'Start the test, expected TPS {test_state.max_tps_per_node} over the next {TEST_TIMEOUT} seconds'
    )
    last_staking = 0
    start_time = time.monotonic()
    while time.monotonic() - start_time < TEST_TIMEOUT:
        # Repeat the staking transactions in case the validator selection algorithm changes.
        staked_time = mocknet.stake_available_amount(
            test_state.node_account,
            last_staking,
        )
        if staked_time is not None:
            last_staking = staked_time

        elapsed_time = time.monotonic() - start_time
        total_tx_sent = mocknet_helpers.throttle_txns(
            load_test_utils.send_random_transactions,
            total_tx_sent,
            elapsed_time,
            test_state,
        )
    logger.info('Stop the test')


if __name__ == '__main__':
    main(sys.argv)
