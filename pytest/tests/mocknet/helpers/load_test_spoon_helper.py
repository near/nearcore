#!/usr/bin/env python3
"""
Generates transactions on a mocknet node.
This file is uploaded to each mocknet node and run there.
"""

import random
import sys
import time
import argparse
import requests

# Don't use the pathlib magic because this file runs on a remote machine.
sys.path.append('lib')
import account
import key as key_mod
import load_test_utils
import mocknet
import mocknet_helpers

from configured_logger import logger


def parse_args():
    parser = argparse.ArgumentParser(
        description='Generates transactions on a mocknet node.')
    parser.add_argument('--node-account-id', required=True, type=str)
    parser.add_argument('--rpc-nodes', required=True, type=str)
    parser.add_argument('--num-nodes', required=True, type=int)
    parser.add_argument('--max-tps', required=True, type=float)

    parser.add_argument('--test-timeout', type=int, default=12 * 60 * 60)
    parser.add_argument(
        '--contract-deploy-time',
        type=int,
        default=10 * mocknet.NUM_ACCOUNTS,
        help=
        'We need to slowly deploy contracts, otherwise we stall out the nodes',
    )

    return parser.parse_args()


def get_test_accounts_from_args(args):

    node_account_id = args.node_account_id
    rpc_nodes = args.rpc_nodes.split(',')
    num_nodes = args.num_nodes
    max_tps = args.max_tps

    logger.info(f'node_account_id: {node_account_id}')
    logger.info(f'rpc_nodes: {rpc_nodes}')
    logger.info(f'num_nodes: {num_nodes}')
    logger.info(f'max_tps: {max_tps}')

    node_account_key = key_mod.Key(
        node_account_id,
        mocknet.PUBLIC_KEY,
        mocknet.SECRET_KEY,
    )
    test_account_keys = [
        key_mod.Key(
            mocknet.load_testing_account_id(node_account_id, i),
            mocknet.PUBLIC_KEY,
            mocknet.SECRET_KEY,
        ) for i in range(mocknet.NUM_ACCOUNTS)
    ]

    base_block_hash = mocknet_helpers.get_latest_block_hash()

    rpc_infos = [(rpc_addr, mocknet_helpers.RPC_PORT) for rpc_addr in rpc_nodes]
    node_account = account.Account(
        node_account_key,
        mocknet_helpers.get_nonce_for_pk(
            node_account_key.account_id,
            node_account_key.pk,
        ),
        base_block_hash,
        rpc_infos=rpc_infos,
    )
    accounts = [
        account.Account(
            key,
            mocknet_helpers.get_nonce_for_pk(
                key.account_id,
                key.pk,
            ),
            base_block_hash,
            rpc_infos=rpc_infos,
        ) for key in test_account_keys
    ]
    max_tps_per_node = max_tps / num_nodes
    return load_test_utils.TestState(
        node_account,
        accounts,
        max_tps_per_node,
        rpc_infos,
    )


def main():
    logger.info(" ".join(sys.argv))

    args = parse_args()
    test_state = get_test_accounts_from_args(args)

    # Ensure load testing contract is deployed to all accounts before
    # starting to send random transactions (ensures we do not try to
    # call the contract before it is deployed).
    delay = args.contract_deploy_time / test_state.num_test_accounts()
    logger.info(f'Start deploying, delay between deployments: {delay}')
    assert delay >= 1

    time.sleep(random.random() * delay)
    start_time = time.monotonic()
    load_test_utils.init_ft(test_state.node_account)
    for i, account in enumerate(test_state.test_accounts):
        logger.info(f'Deploying contract for account {account.key.account_id}')
        mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_deploy_contract_tx(mocknet.WASM_FILENAME))
        load_test_utils.init_ft_account(test_state.node_account, account)
        balance = mocknet_helpers.retry_and_ignore_errors(
            lambda: account.get_amount_yoctonear())
        logger.info(
            f'Account {account.key.account_id} balance after initialization: {balance}'
        )
        time.sleep(max(1.0, start_time + (i + 1) * delay - time.monotonic()))

    logger.info('Done deploying')

    # begin with only transfers for TPS measurement
    total_tx_sent, elapsed_time = 0, 0
    logger.info(
        f'Start the test, expected TPS {test_state.max_tps_per_node} over the next {args.test_timeout} seconds'
    )
    last_staking = 0
    start_time = time.monotonic()
    while time.monotonic() - start_time < args.test_timeout:
        try:
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
        except (
                ConnectionRefusedError,
                requests.exceptions.ConnectionError,
        ) as e:
            # If this happens only occasionally the loop will retry immediately,
            # eventually pick a healthy rpc node and all will be fine. If it
            # happens every time, it indicates that something is wrong and
            # should be visible in grafana tx rate.
            logger.warning(
                f'Error when staking or sending tx. This may happen when the '
                f'selected RPC node is being upgraded. The exception is {e}')
    logger.info('Stop the test')


if __name__ == '__main__':
    main()
