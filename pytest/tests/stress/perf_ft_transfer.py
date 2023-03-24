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
import json
import threading
import queue
import ed25519

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
import utils
import account
import transaction
import key
import account
import mocknet_helpers
from configured_logger import new_logger

DEFAULT_TRANSACTION_TTL_SECONDS = 10
GAS_PER_BLOCK = 10E14
TRANSACTIONS_PER_BLOCK = 121
MAX_INFLIGHT_TRANSACTIONS = 2500
BLOCK_HASH = ''
SEED = random.uniform(0, 0xFFFFFFFF)
logger = new_logger(level = logging.INFO)

class Transaction:
    """
    A transaction future.
    """
    def __init__(self):
        # Number of times we are going to check this transaction for completion before retrying
        # submission
        self.ttl = 0
        self.expiration = 0
        # The transaction id hash
        #
        # str if the transaction has been submitted and may eventually conclude.
        self.transaction_id = None
        # The transaction caller (used for checking the transaction status.
        #
        # `str` if the transaction has been submitted and may eventually conclude.
        self.caller = None
        # The outcome of a successful transaction execution
        self.outcome = None

    def poll(self):
        """
        Returns True if transaction has completed.
        """
        self.ttl = self.expiration - time.time()
        if self.is_complete():
            return True

        # Send the transaction if the previous expired or we didn't send one in the first place.
        if self.transaction_id is None or self.ttl <= 0:
            if self.transaction_id is not None:
                logger.debug(f"transaction {self.transaction_id} expired, submitting a new one!")
            (self.transaction_id, self.caller) = self.send()
            self.expiration = time.time() + DEFAULT_TRANSACTION_TTL_SECONDS
            self.ttl = DEFAULT_TRANSACTION_TTL_SECONDS
            return False # almost guaranteed to not produce any results right now.

        logger.debug(f"checking {self.transaction_id} from {self.caller.key.account_id}")
        try:
            tx_result = self.caller.json_rpc('tx', [self.transaction_id, self.caller.key.account_id])
            if self.is_success(tx_result):
                self.outcome = tx_result
                return True
        except requests.exceptions.ReadTimeout:
            pass
        return False

    def send(self):
        return (self.transaction_id, self.caller)

    def is_complete(self):
        return self.outcome is not None

    def is_success(self, tx_result):
        success = 'error' not in tx_result
        if not success:
            logger.debug(f"transaction {self.transaction_id} for {self.caller} is not successful: {tx_result}")
        return success


class DeployFT(Transaction):
    def __init__(self, account, contract):
        super().__init__()
        self.account = account
        self.contract = contract

    def send(self):
        logger.warning(f"deploying FT to {self.account.key.account_id}")
        result = self.account.send_deploy_contract_tx(self.contract, base_block_hash=BLOCK_HASH)
        return (result["result"], self.account)


class TransferFT(Transaction):
    def __init__(self, ft, sender, recipient, how_much = 1):
        super().__init__()
        self.ft = ft
        self.sender = sender
        self.recipient = recipient
        self.how_much = how_much

    def send(self):
        logger.debug(f"sending {self.how_much} FT from {self.sender.key.account_id} to {self.recipient.key.account_id}")
        args = {
            "receiver_id": self.recipient.key.account_id,
            "amount": str(int(self.how_much)),
        }
        self.sender.prep_tx()
        tx = transaction.sign_function_call_tx(
            self.sender.key,
            self.ft.key.account_id,
            "ft_transfer",
            json.dumps(args).encode('utf-8'),
            # About enough gas per call to fit N such transactions into an average block.
            int(GAS_PER_BLOCK // TRANSACTIONS_PER_BLOCK),
            # Gotta deposit some NEAR for storage?
            1,
            self.sender.nonce,
            BLOCK_HASH)
        result = self.sender.send_tx(tx)
        return (result["result"], self.sender)


class TransferNear(Transaction):
    def __init__(self, sender, recipient_id, how_much = 2.0):
        super().__init__()
        self.recipient_id = recipient_id
        self.sender = sender
        self.how_much = how_much

    def send(self):
        logger.debug(f"sending {self.how_much} NEAR from {self.sender.key.account_id} to {self.recipient_id}")
        result = self.sender.send_transfer_tx(
            self.recipient_id,
            int(self.how_much * 1E24),
            base_block_hash=BLOCK_HASH
        )
        return (result["result"], self.sender)


class InitFT(Transaction):
    def __init__(self, contract):
        super().__init__()
        self.contract = contract

    def send(self):
        args = json.dumps({
            "owner_id": self.contract.key.account_id,
            "total_supply": str(10**33)
        })
        result = self.contract.send_call_contract_tx(
            "new_default_meta",
            args.encode('utf-8'),
            base_block_hash=BLOCK_HASH
        )
        return (result["result"], self.contract)


class InitFTAccount(Transaction):
    def __init__(self, contract, account):
        super().__init__()
        self.contract = contract
        self.account = account

    def send(self):
        args = json.dumps({"account_id": self.account.key.account_id})
        result = self.contract.send_call_contract_raw_tx(
            self.contract.key.account_id,
            "storage_deposit",
            args.encode('utf-8'),
            10**23,
            base_block_hash=BLOCK_HASH,
        )
        return (result["result"], self.contract)


def transaction_executor(tx_queue):
    while True:
        tx = tx_queue.get()
        if not tx.poll():
            # Gotta make sure whoever is pushing to the queue is not filling the queue up fully.
            tx_queue.put(tx)
            if tx.ttl != DEFAULT_TRANSACTION_TTL_SECONDS:
                time.sleep(0.25) # don't spam RPC too hard...


def block_hash_updater(node):
    global BLOCK_HASH
    while True:
        block_hash = node.get_latest_block().hash
        BLOCK_HASH = base58.b58decode(block_hash)
        time.sleep(0.25)


def main():
    parser = argparse.ArgumentParser(description='FT transfer benchmark.')
    parser.add_argument('--fungible-token-wasm', required=True,
        help='Path to the compiled Fungible Token contract')
    parser.add_argument('--setup-cluster', default=False,
        help='Whether to start a dedicated cluster instead of connecting to an existing local node',
        action='store_true')
    parser.add_argument('--contract-key', default=None,
        help='Account to deploy contract to and use as source of NEAR for account creation')
    parser.add_argument('--accounts', default=1000, help='Number of accounts to use')
    parser.add_argument('--no-account-topup', default=False,
        action='store_true', help='Fill accounts with additional NEAR prior to testing')
    parser.add_argument('--shards', default=10, help='number of shards')
    args = parser.parse_args()

    logger.warning(f"SEED is {SEED}")
    rng = random.Random(SEED)

    accounts = []
    for i in range(int(args.accounts)):
        keys = ed25519.create_keypair(entropy=rng.randbytes)
        account_id = keys[1].to_bytes().hex()
        sk = 'ed25519:' + base58.b58encode(keys[0].to_bytes()).decode('ascii')
        pk = 'ed25519:' + base58.b58encode(keys[1].to_bytes()).decode('ascii')
        accounts.append(key.Key(account_id, pk, sk))

    if args.setup_cluster:
        config = cluster.load_config()
        nodes = cluster.start_cluster(2, 0, args.shards, config, [["epoch_length", 100]], {
            shard: { "tracked_shards": list(range(args.shards)) }
            for shard in range(args.shards + 1)
        })
        if args.contract_key is None:
            signer_key = nodes[0].signer_key
        else:
            signer_key = key.Key.from_json_file(args.contract_key)

    else:
        nodes = [
            cluster.RpcNode("127.0.0.1", 3030),
        ]
        # The `nearup` localnet setup stores the keys in this directory.
        if args.contract_key is None:
            key_path = (pathlib.Path.home() / ".near/localnet/node0/shard0_key.json").resolve()
        else:
            key_path = args.contract_key
        signer_key = key.Key.from_json_file(key_path)

    tx_queue = queue.SimpleQueue()
    threading.Thread(target=block_hash_updater, args=(nodes[0],), daemon=True).start()
    while not BLOCK_HASH:
        time.sleep(0.1)
        continue
    threading.Thread(target=transaction_executor, args=(tx_queue,), daemon=True).start()
    init_nonce = mocknet_helpers.get_nonce_for_key(
        signer_key,
        addr=nodes[0].rpc_addr()[0],
        port=nodes[0].rpc_addr()[1],
    )
    contract_account = account.Account(
        signer_key,
        init_nonce,
        '',
        rpc_infos=[node.rpc_addr() for node in nodes]
    )
    tx_queue.put(DeployFT(contract_account, args.fungible_token_wasm))
    wait_empty(tx_queue, "deployment")
    tx_queue.put(InitFT(contract_account))
    wait_empty(tx_queue, "contract initialization")

    if not args.no_account_topup:
        for test_account in accounts:
            tx_queue.put(TransferNear(contract_account, test_account.account_id, 2.0))
        wait_empty(tx_queue, "account creation and top-up")

    # Replace implicit account keys with proper accoutns. We can only do so now, because otherwise
    # the account might not have been created yet and we wouldn't be able to get account's nonce.
    accounts = [
        account.Account(
            key,
                mocknet_helpers.get_nonce_for_key(
                    key, addr=nodes[0].rpc_addr()[0], port=nodes[0].rpc_addr()[1],
            ),
            '',
            rpc_infos=[node.rpc_addr() for node in nodes]
        ) for key in accounts
    ]

    for test_account in accounts:
        tx_queue.put(InitFTAccount(contract_account, test_account))
    wait_empty(tx_queue, "init accounts with the FT contract")

    for test_account in accounts:
        tx_queue.put(TransferFT(contract_account, contract_account, test_account, how_much=1E8))
    wait_empty(tx_queue, "distribution of initial FT")

    transfers = 0
    while True:
        sender, receiver = rng.sample(accounts, k=2)
        tx_queue.put(TransferFT(contract_account, sender, receiver, how_much=1))
        transfers += 1
        if transfers % 10000 == 0:
            logger.info(f"{transfers} so far ({tx_queue.qsize()} in the queue)")
        while tx_queue.qsize() >= MAX_INFLIGHT_TRANSACTIONS:
            time.sleep(0.1)

def wait_empty(queue, why):
    while not queue.empty():
        logger.info(f"waiting for {why} ({queue.qsize()} remain)")
        time.sleep(0.25)
    logger.info(f"wait for {why} completed!")

if __name__ == "__main__":
    main()
