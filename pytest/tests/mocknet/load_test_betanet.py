#!/usr/bin/env python3
import random
import sys
import time
import pathlib
from rc import pmap

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import mocknet_helpers
import account as account_mod
import key as key_mod
from configured_logger import logger

NUM_LETTERS = 26
NUM_ACCOUNTS = NUM_LETTERS * 5


def load_testing_account_id(node_account_id, i):
    letter = i % NUM_LETTERS
    num = i // NUM_LETTERS
    return '%s%02d.%s' % (chr(ord('a') + letter), num, node_account_id)


def send_transfer(account, node_account, base_block_hash=None):
    next_id = random.randrange(NUM_ACCOUNTS)
    dest_account_id = load_testing_account_id(node_account.key.account_id,
                                              next_id)
    mocknet_helpers.retry_and_ignore_errors(lambda: account.send_transfer_tx(
        dest_account_id, base_block_hash=base_block_hash))


def function_call_set_delete_state(account,
                                   i,
                                   node_account,
                                   base_block_hash=None):
    assert i < len(function_call_state)

    if not function_call_state[i]:
        action = "add"
    elif len(function_call_state[i]) >= 100:
        action = "delete"
    else:
        action = random.choice(["add", "delete"])

    if action == "add":
        next_id = random.randrange(NUM_ACCOUNTS)
        next_val = random.randint(0, 1000)
        next_account_id = load_testing_account_id(node_account.key.account_id,
                                                  next_id)
        s = f'{{"account_id": "account_{next_val}", "message":"{next_val}"}}'
        logger.info(
            f'Calling function "set_state" of account {next_account_id} with arguments {s} from account {account.key.account_id}'
        )
        tx_res = mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_call_contract_raw_tx(next_account_id,
                                                      'set_state',
                                                      s.encode('utf-8'),
                                                      0,
                                                      base_block_hash=
                                                      base_block_hash))
        logger.info(
            f'{account.key.account_id} set_state on {next_account_id} {tx_res}')
        function_call_state[i].append((next_id, next_val))
    else:
        assert function_call_state[i]
        item = random.choice(function_call_state[i])
        next_id, next_val = item
        next_account_id = load_testing_account_id(node_account.key.account_id,
                                                  next_id)
        s = f'{{"account_id": "account_{next_val}"}}'
        logger.info(
            f'Calling function "delete_state" of account {next_account_id} with arguments {s} from account {account.key.account_id}'
        )
        tx_res = mocknet_helpers.retry_and_ignore_errors(
            lambda: account.send_call_contract_raw_tx(next_account_id,
                                                      'delete_state',
                                                      s.encode('utf-8'),
                                                      0,
                                                      base_block_hash=
                                                      base_block_hash))
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


def function_call_ft_transfer_call(account, node_account, base_block_hash=None):
    next_id = random.randint(0, NUM_ACCOUNTS - 1)
    dest_account_id = load_testing_account_id(node_account.key.account_id,
                                              next_id)
    contract = node_account.key.account_id

    s = f'{{"receiver_id": "{dest_account_id}", "amount": "3", "msg": "\\"hi\\""}}'
    logger.info(
        f'Calling function "ft_transfer_call" with arguments {s} on account {account.key.account_id} contract {contract} with destination {dest_account_id}'
    )
    tx_res = mocknet_helpers.retry_and_ignore_errors(
        lambda: account.send_call_contract_raw_tx(contract,
                                                  'ft_transfer_call',
                                                  s.encode('utf-8'),
                                                  1,
                                                  base_block_hash=
                                                  base_block_hash))
    logger.info(
        f'{account.key.account_id} ft_transfer to {dest_account_id} {tx_res}')


def random_transaction(account,
                       i,
                       node_account,
                       max_tps_per_node,
                       base_block_hash=None):
    time.sleep(random.random() * NUM_ACCOUNTS / max_tps_per_node / 3)
    choice = random.randint(0, 2)
    if choice == 0:
        logger.info(f'Account {i} transfers')
        send_transfer(account, node_account, base_block_hash=base_block_hash)
    elif choice == 1:
        function_call_set_delete_state(account,
                                       i,
                                       node_account,
                                       base_block_hash=base_block_hash)
    elif choice == 2:
        function_call_ft_transfer_call(account,
                                       node_account,
                                       base_block_hash=base_block_hash)


def send_random_transactions(node_account, test_accounts, max_tps_per_node):
    logger.info("===========================================")
    logger.info("New iteration of 'send_random_transactions'")
    base_block_hash = mocknet_helpers.get_latest_block_hash()
    pmap(
        lambda index_and_account: random_transaction(index_and_account[1],
                                                     index_and_account[0],
                                                     node_account,
                                                     max_tps_per_node,
                                                     base_block_hash=
                                                     base_block_hash),
        enumerate(test_accounts))


def init_ft(node_account):
    tx_res = node_account.send_deploy_contract_tx(
        '/home/ubuntu/fungible_token.wasm')
    logger.info(f'ft deployment {tx_res}')
    mocknet_helpers.wait_at_least_one_block()

    s = f'{{"owner_id": "{node_account.key.account_id}", "total_supply": "{10**33}"}}'
    tx_res = node_account.send_call_contract_raw_tx(node_account.key.account_id,
                                                    'new_default_meta',
                                                    s.encode('utf-8'), 0)
    logger.info(f'ft new_default_meta {tx_res}')


def init_ft_account(node_account, account):
    s = f'{{"account_id": "{account.key.account_id}"}}'
    tx_res = account.send_call_contract_raw_tx(node_account.key.account_id,
                                               'storage_deposit',
                                               s.encode('utf-8'),
                                               (10**24) // 800)
    logger.info(f'Account {account.key.account_id} storage_deposit {tx_res}')

    # The next transaction depends on the previous transaction succeeded.
    # Sleeping for 1 second is the poor man's solution for waiting for that transaction to succeed.
    # This works because the contracts are being deployed slow enough to keep block production above 1 bps.
    mocknet_helpers.wait_at_least_one_block()

    s = f'{{"receiver_id": "{account.key.account_id}", "amount": "{10**18}"}}'
    logger.info(
        f'Calling function "ft_transfer" with arguments {s} on account {account.key.account_id}'
    )
    tx_res = node_account.send_call_contract_raw_tx(node_account.key.account_id,
                                                    'ft_transfer',
                                                    s.encode('utf-8'), 1)
    logger.info(
        f'{node_account.key.account_id} ft_transfer to {account.key.account_id} {tx_res}'
    )


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
        key_mod.Key(load_testing_account_id(node_account_id, i), pk, sk)
        for i in range(NUM_ACCOUNTS)
    ]

    base_block_hash = mocknet_helpers.get_latest_block_hash()
    node_account = account_mod.Account(node_account_key,
                                       mocknet_helpers.get_nonce_for_pk(
                                           node_account_key.account_id,
                                           node_account_key.pk),
                                       base_block_hash,
                                       rpc_infos=rpc_infos)

    if need_deploy:
        init_ft(node_account)

    accounts = []
    for key in test_account_keys:
        account = account_mod.Account(key,
                                      mocknet_helpers.get_nonce_for_pk(
                                          key.account_id, key.pk),
                                      base_block_hash,
                                      rpc_infos=rpc_infos)
        accounts.append(account)
        if need_deploy:
            logger.info(f'Deploying contract for account {key.account_id}')
            tx_res = account.send_deploy_contract_tx('betanet_state.wasm')
            logger.info(f'Deploying result: {tx_res}')
            init_ft_account(node_account, account)
            logger.info(
                f'Account {key.account_id} balance after initialization: {account.get_amount_yoctonear()}'
            )
            mocknet_helpers.wait_at_least_one_block()

    return node_account, accounts, max_tps


def main(argv):
    logger.info(argv)
    (node_account, test_accounts,
     max_tps_per_node) = get_test_accounts_from_args(argv)

    global function_call_state
    function_call_state = [[]] * NUM_ACCOUNTS

    total_tx_sent = 0
    start_time = time.monotonic()
    while True:
        elapsed_time = time.monotonic() - start_time
        total_tx_sent = mocknet_helpers.throttle_txns(
            send_random_transactions, total_tx_sent, elapsed_time,
            max_tps_per_node, node_account, test_accounts)


if __name__ == '__main__':
    main(sys.argv)
