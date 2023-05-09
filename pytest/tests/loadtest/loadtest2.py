#!/usr/bin/env python3
"""
This is a benchmark in which a network with a single fungible_token contract is
deployed, and then a variable number of users (see `N_ACCOUNTS`) send each other
those fungible tokens.

At the time this benchmark is written, the intent is to observe the node metrics
and traces for the block duration, potentially adding any additional
instrumentation as needed.

To run:

```
env NEAR_ROOT=../target/release/ \
    python3 tests/loadtest/loadtest2.py \
    --fungible-token-wasm=$PWD/../../FT/res/fungible_token.wasm \
    --setup-cluster --accounts=1000 --executors=4
```
"""

import argparse
import sys
import os
import time
import pathlib
import base58
import requests
import random
import logging
import json
import multiprocessing
import multiprocessing.queues
import ctypes
import ed25519
import queue
import string

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
MAX_INFLIGHT_TRANSACTIONS_PER_EXECUTOR = 1000
SEED = random.uniform(0, 0xFFFFFFFF)
logger = new_logger(level=logging.INFO)


class Transaction:
    """
    A transaction future.
    """

    ID = 0

    def __init__(self):
        self.id = Transaction.ID
        Transaction.ID += 1

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

    def poll(self, node, block_hash):
        """
        Returns True if transaction has completed.
        """
        if self.is_complete():
            return True
        # Send the transaction if the previous expired or we didn't send one in the first place.
        if self.transaction_id is None or self.ttl <= 0:
            if self.transaction_id is not None:
                logger.warning(
                    f"transaction {self.transaction_id} expired, submitting a new one!"
                )
            (self.transaction_id, self.caller) = self.send(node, block_hash)
            self.expiration = time.time() + DEFAULT_TRANSACTION_TTL_SECONDS
            self.ttl = DEFAULT_TRANSACTION_TTL_SECONDS
            return False  # almost guaranteed to not produce any results right now.
        caller = ACCOUNTS[self.caller]
        logger.debug(
            f"checking {self.transaction_id} from {caller.key.account_id}")
        tx_result = node.json_rpc('tx',
                                  [self.transaction_id, caller.key.account_id])
        if self.is_success(tx_result):
            self.outcome = tx_result
            return True
        return False

    def send(self, block_hash):
        return (self.transaction_id, self.caller)

    def is_complete(self):
        return self.outcome is not None

    def is_success(self, tx_result):
        success = 'error' not in tx_result
        if not success:
            logger.debug(
                f"transaction {self.transaction_id} for {self.caller} is not successful: {tx_result}"
            )
        # only set TTL if we managed to check for success or failure...
        self.ttl = self.expiration - time.time()
        return success


class DeployFT(Transaction):

    def __init__(self, account, contract):
        super().__init__()
        self.account = account
        self.contract = contract

    def send(self, node, block_hash):
        account = ACCOUNTS[self.account]
        logger.warning(f"deploying FT to {account.key.account_id}")
        wasm_binary = utils.load_binary_file(self.contract)
        tx = transaction.sign_deploy_contract_tx(account.key, wasm_binary,
                                                 account.use_nonce(),
                                                 block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.account)


class TransferFT(Transaction):

    def __init__(self, ft, sender, recipient, how_much=1, tgas=300):
        super().__init__()
        self.ft = ft
        self.sender = sender
        self.recipient = recipient
        self.how_much = how_much
        self.tgas = tgas

    def send(self, node, block_hash):
        (ft, sender, recipient
        ) = ACCOUNTS[self.ft], ACCOUNTS[self.sender], ACCOUNTS[self.recipient]
        logger.debug(
            f"sending {self.how_much} FT from {sender.key.account_id} to {recipient.key.account_id}"
        )
        args = {
            "receiver_id": recipient.key.account_id,
            "amount": str(int(self.how_much)),
        }
        tx = transaction.sign_function_call_tx(
            sender.key,
            ft.key.account_id,
            "ft_transfer",
            json.dumps(args).encode('utf-8'),
            # About enough gas per call to fit N such transactions into an average block.
            self.tgas * account.TGAS,
            # Gotta attach exactly 1 yoctoNEAR according to NEP-141 to avoid calls from restricted access keys
            1,
            sender.use_nonce(),
            block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class TransferNear(Transaction):

    def __init__(self, sender, recipient_id, how_much=2.0):
        super().__init__()
        self.recipient_id = recipient_id
        self.sender = sender
        self.how_much = how_much

    def send(self, node, block_hash):
        sender = ACCOUNTS[self.sender]
        logger.debug(
            f"sending {self.how_much} NEAR from {sender.key.account_id} to {self.recipient_id}"
        )
        tx = transaction.sign_payment_tx(sender.key, self.recipient_id,
                                         int(self.how_much * 1E24),
                                         sender.use_nonce(), block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class CreateSubAccount(Transaction):

    def __init__(self, sender, sub, balance=50.0):
        super().__init__()
        self.sender = sender
        self.sub = sub
        self.balance = balance

    def send(self, node, block_hash):
        sender = ACCOUNTS[self.sender]
        sub = ACCOUNTS[self.sub]
        new_account_id = f"{sub.key.account_id}.{sender.key.account_id}"
        logger.debug(f"creating {new_account_id}")
        tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
            sender.key, sub.key.account_id, sub.key, int(self.balance * 1E24),
            sender.use_nonce(), block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class InitFT(Transaction):

    def __init__(self, contract):
        super().__init__()
        self.contract = contract

    def send(self, node, block_hash):
        contract = ACCOUNTS[self.contract]
        args = json.dumps({
            "owner_id": contract.key.account_id,
            "total_supply": str(10**33)
        })
        tx = transaction.sign_function_call_tx(contract.key,
                                               contract.key.account_id,
                                               "new_default_meta",
                                               args.encode('utf-8'), int(3E14),
                                               0, contract.use_nonce(),
                                               block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.contract)


class InitFTAccount(Transaction):

    def __init__(self, contract, account):
        super().__init__()
        self.contract = contract
        self.account = account

    def send(self, node, block_hash):
        contract, account = ACCOUNTS[self.contract], ACCOUNTS[self.account]
        args = json.dumps({"account_id": account.key.account_id})
        tx = transaction.sign_function_call_tx(contract.key,
                                               contract.key.account_id,
                                               "storage_deposit",
                                               args.encode('utf-8'), int(3E14),
                                               int(1E23), contract.use_nonce(),
                                               block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.contract)


class TxQueue(multiprocessing.queues.Queue):

    def __init__(self, size, *args, **kwargs):
        super().__init__(size,
                         ctx=multiprocessing.get_context(),
                         *args,
                         **kwargs)
        self.pending = multiprocessing.Value(ctypes.c_ulong, 0)

    def add(self, tx):
        with self.pending.get_lock():
            self.pending.value += 1
        self.put(tx)

    def complete(self):
        with self.pending.get_lock():
            self.pending.value -= 1


class Account:

    def __init__(self, key):
        self.key = key
        self.nonce = multiprocessing.Value(ctypes.c_ulong, 0)

    def refresh_nonce(self, node):
        with self.nonce.get_lock():
            self.nonce.value = mocknet_helpers.get_nonce_for_key(
                self.key,
                addr=node.rpc_addr()[0],
                port=node.rpc_addr()[1],
            )

    def use_nonce(self):
        with self.nonce.get_lock():
            new_nonce = self.nonce.value + 1
            self.nonce.value = new_nonce
            return new_nonce


def transaction_executor(nodes, tx_queue, accounts):
    global ACCOUNTS
    ACCOUNTS = accounts
    last_block_hash_update = 0
    my_transactions = queue.SimpleQueue()
    rng = random.Random()
    while True:
        node = rng.choice(nodes)
        try:
            now = time.time()
            if now - last_block_hash_update >= 0.5:
                block_hash = base58.b58decode(node.get_latest_block().hash)
                last_block_hash_update = now
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError):
            continue

        while my_transactions.qsize() < MAX_INFLIGHT_TRANSACTIONS_PER_EXECUTOR:
            try:
                tx = tx_queue.get_nowait()
            except queue.Empty:
                break
            # Send out the transaction immediately.
            try:
                tx.poll(node, block_hash)
            except (requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError):
                pass
            my_transactions.put(tx)

        try:
            tx = my_transactions.get_nowait()
        except queue.Empty:
            time.sleep(0.1)
            continue
        try:
            poll_result = tx.poll(node, block_hash)
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError):
            my_transactions.put(tx)
            continue
        if not poll_result:
            my_transactions.put(tx)
            if tx.ttl != DEFAULT_TRANSACTION_TTL_SECONDS:
                time.sleep(0.1)  # don't spam RPC too hard...
        else:
            tx_queue.complete()


def main():
    parser = argparse.ArgumentParser(description='FT transfer benchmark.')
    parser.add_argument('--fungible-token-wasm',
                        required=True,
                        help='Path to the compiled Fungible Token contract')
    parser.add_argument(
        '--setup-cluster',
        default=False,
        help=
        'Whether to start a dedicated cluster instead of connecting to an existing local node',
        action='store_true')
    parser.add_argument(
        '--contracts',
        default='0,2,4,6,8,a,c,e',
        help=
        'Number of contract accounts, or alternatively list of subnames, separated by commas'
    )
    parser.add_argument(
        '--contract-key',
        required='--setup-cluster' not in sys.argv,
        help=
        'account to deploy contract to and use as source of NEAR for account creation'
    )
    parser.add_argument('--accounts',
                        default=1000,
                        help='Number of accounts to use')
    parser.add_argument(
        '--no-account-topup',
        default=False,
        action='store_true',
        help='Fill accounts with additional NEAR prior to testing')
    parser.add_argument('--shards', default=10, help='number of shards')
    parser.add_argument('--executors',
                        default=2,
                        help='number of transaction executors')
    parser.add_argument('--tx-tgas',
                        default=30,
                        help='amount of Tgas to attach to each transaction')
    args = parser.parse_args()

    logger.warning(f"SEED is {SEED}")
    rng = random.Random(SEED)

    if args.setup_cluster:
        config = cluster.load_config()
        nodes = cluster.start_cluster(
            2, 0, args.shards, config, [["epoch_length", 100]], {
                shard: {
                    "tracked_shards": list(range(args.shards))
                } for shard in range(args.shards + 1)
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
        key_path = args.contract_key
        signer_key = key.Key.from_json_file(key_path)

    ACCOUNTS = []
    ACCOUNTS.append(Account(signer_key))
    ACCOUNTS[0].refresh_nonce(nodes[0])
    funding_account = 0
    start_of_accounts = len(ACCOUNTS) - 1
    contract_accounts = []

    try:
        for sub in sorted(
                rng.sample(string.ascii_lowercase + string.digits,
                           k=int(args.contracts))):
            funding_key = ACCOUNTS[funding_account].key
            sub_key = key.Key(f"{sub}.{funding_key.account_id}", funding_key.pk,
                              funding_key.sk)
            contract_accounts.append(len(ACCOUNTS))
            ACCOUNTS.append(Account(sub_key))
    except ValueError:
        for sub in args.contracts.split(","):
            funding_key = ACCOUNTS[funding_account].key
            sub_key = key.Key(f"{sub}.{funding_key.account_id}", funding_key.pk,
                              funding_key.sk)
            contract_accounts.append(len(ACCOUNTS))
            ACCOUNTS.append(Account(sub_key))

    for i in range(int(args.accounts)):
        keys = ed25519.create_keypair(entropy=rng.randbytes)
        account_id = keys[1].to_bytes().hex()
        sk = 'ed25519:' + base58.b58encode(keys[0].to_bytes()).decode('ascii')
        pk = 'ed25519:' + base58.b58encode(keys[1].to_bytes()).decode('ascii')
        ACCOUNTS.append(Account(key.Key(account_id, pk, sk)))

    executors = int(args.executors)
    queue_size = 16 + max(MAX_INFLIGHT_TRANSACTIONS_PER_EXECUTOR,
                          int(args.accounts) * len(contract_accounts))
    tx_queue = TxQueue(queue_size)
    subargs = (
        nodes,
        tx_queue,
        ACCOUNTS,
    )
    for executor in range(executors):
        multiprocessing.Process(target=transaction_executor,
                                args=subargs,
                                daemon=True).start()

    for contract_account in contract_accounts:
        tx_queue.add(CreateSubAccount(funding_account, contract_account))
    wait_empty(tx_queue, "creating contract sub accounts")
    for contract_account in contract_accounts:
        ACCOUNTS[contract_account].refresh_nonce(nodes[0])
        tx_queue.add(DeployFT(contract_account, args.fungible_token_wasm))
    wait_empty(tx_queue, "deployment")
    for contract_account in contract_accounts:
        tx_queue.add(InitFT(contract_account))
    wait_empty(tx_queue, "contract initialization")

    if not args.no_account_topup:
        for test_account in ACCOUNTS[start_of_accounts:]:
            tx_queue.add(
                TransferNear(funding_account, test_account.key.account_id, 2.0))
        wait_empty(tx_queue, "account creation and top-up")

    for contract_account in contract_accounts:
        for test_account_idx in range(start_of_accounts, len(ACCOUNTS)):
            # Initialize nonces for all real accounts. But we only want to do that for the first
            # iteration... Otherwise there's a risk of a race. And O(n^2) doesn't help...
            if contract_account == contract_accounts[0]:
                ACCOUNTS[test_account_idx].refresh_nonce(nodes[0])
            tx_queue.add(InitFTAccount(contract_account, test_account_idx))
    wait_empty(tx_queue, "registeration of accounts with the FT contracts")

    for contract_account in contract_accounts:
        for test_account_idx in range(start_of_accounts, len(ACCOUNTS)):
            tx_queue.add(
                TransferFT(contract_account,
                           contract_account,
                           test_account_idx,
                           how_much=1E8))
    wait_empty(tx_queue, "distribution of initial FT")

    transfers = 0
    while True:
        sender_idx, receiver_idx = rng.sample(range(start_of_accounts,
                                                    len(ACCOUNTS)),
                                              k=2)
        ft_contract = rng.choice(contract_accounts)
        tgas = int(args.tx_tgas)
        tx_queue.add(
            TransferFT(ft_contract,
                       sender_idx,
                       receiver_idx,
                       how_much=1,
                       tgas=tgas))
        transfers += 1
        if transfers % 10000 == 0:
            logger.info(
                f"{transfers} so far ({tx_queue.pending.value} pending)")
        while tx_queue.pending.value >= MAX_INFLIGHT_TRANSACTIONS_PER_EXECUTOR * executors:
            time.sleep(0.25)


def wait_empty(queue, why):
    with queue.pending.get_lock():
        remaining = queue.pending.value
    while remaining != 0:
        logger.info(f"waiting for {why} ({remaining} remain)")
        time.sleep(0.5)
        with queue.pending.get_lock():
            remaining = queue.pending.value
    logger.info(f"wait for {why} completed!")


if __name__ == "__main__":
    main()
