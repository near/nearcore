#!/usr/bin/env python3

"""
This is a benchmark in which a network with a single fungible_token contract is
deployed, and then a variable number of users (see `N_ACCOUNTS`) send each other
them fungible tokens.

At the time this benchmark is written, the intent is to observe the node metrics
and traces for the block duration, potentially adding any additional
instrumentation as needed.

To run:

```
env NEAR_ROOT=../target/release/ \
    python3 tests/stress/perf_ft_transfer.py \
      --fungible-token-wasm=$HOME/FT/res/fungible_token.wasm
```
"""

import argparse
import sys
import os
import time
import pathlib
import base58
import itertools
import requests
import random
import logging

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
import utils
import account
import transaction
import key
import account
import mocknet_helpers
from configured_logger import new_logger

N_ACCOUNTS = 1000
MAX_INFLIGHT_TXS = 1000 # aka. TXs per test iteration
GAS_PER_BLOCK = 10E14
TRANSACTIONS_PER_BLOCK = 101
SEED = random.uniform(0, 0xFFFFFFFF)
logger = new_logger(level = logging.INFO)

def main():
    parser = argparse.ArgumentParser(description='FT transfer benchmark.')
    parser.add_argument('--fungible-token-wasm', required=True,
        help='Path to the compiled Fungible Token contract')
    parser.add_argument('--setup-cluster', default=False,
        help='Whether to start a dedicated cluster instead of connecting to an existing local node',
        action='store_true')
    args = parser.parse_args()

    logger.warn(f"SEED is {SEED}")
    rng = random.Random(SEED)

    if args.setup_cluster:
        config = cluster.load_config()
        nodes = cluster.start_cluster(2, 0, 4, config, [["epoch_length", 100]], {
            0: {
                "tracked_shards": [0, 1, 2, 3],
            }
        })
        signer_key = nodes[0].signer_key
    else:
        nodes = [
            cluster.RpcNode("127.0.0.1", 3030),
        ]
        # The `nearup` localnet setup stores the keys in this directory.
        key_path = pathlib.Path.home() / ".near/localnet/node0/shard0_key.json"
        signer_key = key.Key.from_json_file(key_path.resolve())

    block_hash = nodes[0].get_latest_block().hash
    block_hash = base58.b58decode(block_hash)
    init_nonce = mocknet_helpers.get_nonce_for_pk(
            signer_key.account_id,
            signer_key.pk,
            port = nodes[0].rpc_port
        )
    node0_acct = account.Account(signer_key,
        init_nonce,
        block_hash,
        rpc_infos=[("127.0.0.1", nodes[0].rpc_port)])

    contract_tx = node0_acct.send_deploy_contract_tx(args.fungible_token_wasm)
    assert wait_tx(nodes[0], contract_tx["result"], node0_acct.key.account_id)

    accounts = []
    for i in range(N_ACCOUNTS):
        k = key.Key.implicit_account()
        accounts.append(account.Account(k, 0, block_hash, rpc_info=("127.0.0.1", nodes[0].rpc_port)))
    for_each_account_until_all_succeed(nodes[0], accounts, 10, create_account_factory(node0_acct))

    s = f'{{"owner_id": "{node0_acct.key.account_id}", "total_supply": "{10**33}"}}'
    tx = node0_acct.send_call_contract_tx("new_default_meta", s.encode('utf-8'))
    assert wait_tx(nodes[0], tx["result"], node0_acct.key.account_id)

    for acct in accounts:
        acct.nonce = mocknet_helpers.get_nonce_for_pk(
            acct.key.account_id,
            acct.key.pk,
            port = nodes[0].rpc_port
        )
    for_each_account_until_all_succeed(nodes[0], accounts, 10, ft_account_init_factory(node0_acct))
    for_each_account_until_all_succeed(nodes[0], accounts, 10, transfer_tokens_factory(node0_acct, node0_acct))
    # Setup is complete at this point.

    for iteration in itertools.count():
        block_hash = nodes[0].get_latest_block().hash
        block_hash = base58.b58decode(block_hash)
        logger.info(f"TEST ITERATION {iteration}")
        txs = set()
        failed = 0
        for i in range(MAX_INFLIGHT_TXS):
            sender, receiver = None, None
            while sender == receiver:
                [sender, receiver] = rng.choices(accounts, k=2)
            sender.base_block_hash = block_hash
            receiver.base_block_hash = block_hash
            txs.add(transfer_tokens_factory(node0_acct, sender)(receiver))
        for (tx, recipient) in txs:
            failed += int(not wait_tx(nodes[0], tx, recipient))
        logger.info(f"{failed} tx failures")


def ft_account_init_factory(node):
    def init_ft_account(acct):
        s = f'{{"account_id": "{acct.key.account_id}"}}'
        result = acct.send_call_contract_raw_tx(
            node.key.account_id,
            "storage_deposit",
            s.encode('utf-8'),
            10**23,
        )
        return result["result"], node.key.account_id
    return init_ft_account


def transfer_tokens_factory(node, sender):
    def do_it(to_whom):
        s = f'{{"receiver_id": "{to_whom.key.account_id}", "amount": "12349876"}}'
        sender.prep_tx()
        tx = transaction.sign_function_call_tx(
            sender.key,
            node.key.account_id,
            "ft_transfer",
            s.encode('utf-8'),
            # About enough gas per call to fit N such transactions into an average block.
            int(GAS_PER_BLOCK // TRANSACTIONS_PER_BLOCK),
            1,
            sender.nonce,
            sender.base_block_hash)
        while True:
            try:
                result = sender.send_tx(tx)
                break
            except requests.exceptions.ReadTimeout:
                pass
        return result["result"], sender.key.account_id
    return do_it


def create_account_factory(sender):
    def create_account(acct):
        result = sender.send_transfer_tx(acct.key.account_id, 10**24)
        return result["result"], acct.key.account_id
    return create_account


def for_each_account_until_all_succeed(node, accounts, limit, send_tx):
    accounts_to_succeed = set(accounts)
    while accounts_to_succeed:
        waitlist = dict()
        while accounts_to_succeed:
            account = accounts_to_succeed.pop()
            (tx_id, recipient) = send_tx(account)
            waitlist[(tx_id, recipient)] = account
        while waitlist:
            (tx_id, recipient), v = waitlist.popitem()
            if not wait_tx(node, tx_id, recipient):
                accounts_to_succeed.add(v)


def wait_tx_once(node, tx_id, recipient):
    logger.debug(f"checking for {tx_id} sent to {recipient}")
    try:
        tx_result = node.get_tx(tx_id, recipient)
    except requests.exceptions.ReadTimeout:
        return False
    logger.debug(tx_result)
    if 'error' in tx_result:
        return False
    else:
        return True


def wait_tx(node, tx_id, recipient):
    try:
        if wait_tx_once(node, tx_id, recipient):
            return True
        for height, hsh in itertools.islice(utils.poll_blocks(node, timeout=10**9), 10):
            if wait_tx_once(node, tx_id, recipient):
                return True
        return False
    except requests.exceptions.ReadTimeout:
        # We can't communicate with the node, just say that the transaction failed...
        return False

if __name__ == "__main__":
    main()
