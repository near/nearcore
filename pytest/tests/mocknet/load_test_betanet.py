#!/usr/bin/env python3
import pathlib
import random
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import account as account_mod
import key as key_mod
import mocknet
import mocknet_helpers
from helpers import load_test_utils

from configured_logger import logger

NUM_LETTERS = 26
NUM_ACCOUNTS = NUM_LETTERS * 5


def get_test_accounts_from_args(argv):
    assert len(argv) == 7
    node_account_id = argv[1]
    pk = argv[2]
    sk = argv[3]
    assert argv[4]
    rpc_nodes = argv[4].split(',')
    logger.info(f'rpc_nodes: {rpc_nodes}')
    max_tps = float(argv[5])
    need_deploy = (argv[6] == 'deploy')

    logger.info(f'need_deploy: {need_deploy}')

    rpc_infos = [(rpc_addr, mocknet_helpers.RPC_PORT) for rpc_addr in rpc_nodes]

    node_account_key = key_mod.Key(node_account_id, pk, sk)
    test_account_keys = [
        key_mod.Key(mocknet.load_testing_account_id(node_account_id, i), pk, sk)
        for i in range(NUM_ACCOUNTS)
    ]

    base_block_hash = mocknet_helpers.get_latest_block_hash(
        addr=random.choice(rpc_nodes))
    node_account = account_mod.Account(node_account_key,
                                       mocknet_helpers.get_nonce_for_pk(
                                           node_account_key.account_id,
                                           node_account_key.pk,
                                           addr=random.choice(rpc_nodes)),
                                       base_block_hash,
                                       rpc_infos=rpc_infos)

    test_accounts = []
    for key in test_account_keys:
        account = account_mod.Account(key,
                                      mocknet_helpers.get_nonce_for_pk(
                                          key.account_id,
                                          key.pk,
                                          addr=random.choice(rpc_nodes)),
                                      base_block_hash,
                                      rpc_infos=rpc_infos)
        test_accounts.append(account)

    return load_test_utils.TestState(node_account, test_accounts, max_tps,
                                     rpc_infos), need_deploy


def main(argv):
    logger.info(argv)
    test_state, need_deploy = get_test_accounts_from_args(argv)

    if need_deploy:
        load_test_utils.init_ft(test_state.node_account)
        for account in test_state.test_accounts:
            logger.info(
                f'Deploying contract for account {account.key.account_id}')
            tx_res = account.send_deploy_contract_tx('betanet_state.wasm')
            logger.info(f'Deploying result: {tx_res}')
            load_test_utils.init_ft_account(test_state.node_account, account)
            logger.info(
                f'Account {account.key.account_id} balance after initialization: {account.get_amount_yoctonear()}'
            )
            mocknet_helpers.wait_at_least_one_block()

    total_tx_sent = 0
    start_time = time.monotonic()
    while True:
        elapsed_time = time.monotonic() - start_time
        total_tx_sent = mocknet_helpers.throttle_txns(
            load_test_utils.send_random_transactions, total_tx_sent,
            elapsed_time, test_state)


if __name__ == '__main__':
    main(sys.argv)
