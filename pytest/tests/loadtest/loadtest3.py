#!/usr/bin/env python3
"""
WIP test: Not intended to ever go to master, but a snapshot of a working
prototype that will be transformed into proper locust files.
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
from locust import HttpUser, User, between, task, events

DEFAULT_TRANSACTION_TTL_SECONDS = 20
MAX_INFLIGHT_TRANSACTIONS_PER_EXECUTOR = 40
SEED = random.uniform(0, 0xFFFFFFFF)
TOTAL_TX_NUM = 50_000
logger = new_logger(level=logging.WARN)

FUNDING_ACCOUNT = None
FT_ACCOUNT = None

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

    def finish(self, block_hash):
        return None

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


class Deploy(Transaction):

    def __init__(self, account, contract, name):
        super().__init__()
        self.account = account
        self.contract = contract
        self.name = name

    def finish(self, block_hash):
        account = self.account
        logger.info(f"deploying {self.name} to {account.key.account_id}")
        wasm_binary = utils.load_binary_file(self.contract)
        tx = transaction.sign_deploy_contract_tx(account.key, wasm_binary,
                                                 account.use_nonce(),
                                                 block_hash)
        return tx
        
    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.account)


class TransferFT(Transaction):

    def __init__(self, ft, sender, recipient_id, how_much=1, tgas=300):
        super().__init__()
        self.ft = ft
        self.sender = sender
        self.recipient_id = recipient_id
        self.how_much = how_much
        self.tgas = tgas

    def finish(self, block_hash):
        (ft, sender, recipient_id
        ) = self.ft, self.sender, self.recipient_id
        logger.debug(
            f"sending {self.how_much} FT from {sender.key.account_id} to {recipient_id}"
        )
        args = {
            "receiver_id": recipient_id,
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
        return tx
        
    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class TransferNear(Transaction):

    def __init__(self, sender, recipient_id, how_much=2.0):
        super().__init__()
        self.recipient_id = recipient_id
        self.sender = sender
        self.how_much = how_much

    def finish(self, block_hash):
        sender = self.sender
        logger.debug(
            f"sending {self.how_much} NEAR from {sender.key.account_id} to {self.recipient_id}"
        )
        tx = transaction.sign_payment_tx(sender.key, self.recipient_id,
                                         int(self.how_much * 1E24),
                                         sender.use_nonce(), block_hash)
        return tx

    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class CreateSubAccount(Transaction):

    def __init__(self, sender, sub_key, balance=50.0):
        super().__init__()
        self.sender = sender
        self.sub_key = sub_key
        self.balance = balance

    def finish(self, block_hash):
        sender = self.sender
        sub = self.sub_key
        logger.debug(f"creating {sub.account_id}")
        tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
            sender.key, sub.account_id, sub, int(self.balance * 1E24),
            sender.use_nonce(), block_hash)
        return tx
    
    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class InitFT(Transaction):

    def __init__(self, contract):
        super().__init__()
        self.contract = contract

    def finish(self, block_hash):
        contract = self.contract
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
        return tx

    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.contract)


class InitFTAccount(Transaction):

    def __init__(self, contract, account):
        super().__init__()
        self.contract = contract
        self.account = account

    def finish(self, block_hash):
        contract, account = self.contract, self.account
        args = json.dumps({"account_id": account.key.account_id})
        tx = transaction.sign_function_call_tx(contract.key,
                                               contract.key.account_id,
                                               "storage_deposit",
                                               args.encode('utf-8'), int(3E14),
                                               int(1E23), contract.use_nonce(),
                                               block_hash)
        return tx

    def send(self, node, block_hash):
        tx = self.finish(block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.contract)


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



def main():
    """
    Setup a local cluster to run locust against
    """
    parser = argparse.ArgumentParser(description='FT transfer benchmark.')
    parser.add_argument('--shards', default=10, help='number of shards')
    args = parser.parse_args()

    logger.warning(f"SEED is {SEED}")
    rng = random.Random(SEED)

    
    config = cluster.load_config()
    nodes = cluster.start_cluster(
        2, 0, args.shards, config, [["epoch_length", 100]], {
            shard: {
                "tracked_shards": list(range(int(args.shards)))
            } for shard in range(int(args.shards) + 1)
        })
    
    for node in nodes:
        print(f"RPC node listening on port {node.rpc_port}")
    
    while True:
        continue

class NearUser(HttpUser):
    abstract = True
    id_counter = 0

    @classmethod
    def get_next_id(cls):
        cls.id_counter += 1
        return cls.id_counter

    def __init__(self, environment):
        super().__init__(environment)
        host, port = self.host.split(":")
        self.node = cluster.RpcNode(host, port)
        self.node.session = self.client
        self.id = NearUser.get_next_id()

    def on_start(self):
        while FT_ACCOUNT is None:
            logger.debug("init not ready, yet")
            time.sleep(1)
        self.funding_account = FUNDING_ACCOUNT
        self.contract_account = FT_ACCOUNT
        # TODO: Random prefix for better trie spreading
        self.account_id = f"user{self.id}.{self.funding_account.key.account_id}"
        self.account = Account(key.Key.from_random(self.account_id))
        self.send_tx_retry(CreateSubAccount(self.funding_account, self.account.key, balance=5000.0))
        self.account.refresh_nonce(self.node)

    def send_tx(self, tx: Transaction):
        block_hash = base58.b58decode(self.node.get_latest_block().hash)
        signed_tx = tx.finish(block_hash)
        tx_result = self.node.send_tx_and_wait(signed_tx, timeout=DEFAULT_TRANSACTION_TTL_SECONDS)
        success = "error" not in tx_result
        if success:
            logger.info(
                f"SUCCESS {tx.transaction_id} for {self.account_id} is successful: {tx_result}"
            )
            return True, tx_result
        elif "UNKNOWN_TRANSACTION" in tx_result:
            logger.debug(
                f"transaction {tx.transaction_id} for {self.account_id} timed out / failed to be accepted"
            )
        else:
            logger.warn(
                f"transaction {tx.transaction_id} for {self.account_id} is not successful: {tx_result}"
            )
        return False, tx_result

    def send_tx_retry(self, tx: Transaction):
        ok, tx_result = self.send_tx(tx)
        while not ok:
            logger.warn(f"transaction {tx.transaction_id} for {self.account_id} is not successful: {tx_result}")
            time.sleep(0.25)
            ok, tx_result = self.send_tx(tx)

    def send_tx_retry_old(self, tx: Transaction):
        while True:
            block_hash = base58.b58decode(self.node.get_latest_block().hash)
            (tx.transaction_id, _) = tx.send(self.node, block_hash)
            for _ in range(DEFAULT_TRANSACTION_TTL_SECONDS):
                time.sleep(0.25)
                tx_result = self.node.json_rpc("tx", [tx.transaction_id, self.account_id])
                success = "error" not in tx_result
                if success:
                    logger.info(
                        f"SUCCESS {tx.transaction_id} for {self.account_id} is successful: {tx_result}"
                    )
                    return
                elif "UNKNOWN_TRANSACTION" in tx_result:
                    logger.warn(
                        f"transaction {tx.transaction_id} for {self.account_id} is not successful: {tx_result}"
                    )
                else:
                    logger.debug(
                        f"transaction {tx.transaction_id} for {self.account_id} not done yet"
                    )
            logger.warning(f"transaction {tx.transaction_id} expired, submitting a new one!")



class FtTransferUser(NearUser):
    wait_time = between(1, 5) # random pause between transactions

    @task
    def ft_transfer(self):
        logger.info(f"START FT TRANSFER {self.id}")
        # self.account.refresh_nonce(todo: node)
        receiver = self.contract_account.key.account_id# TODO
        self.send_tx(
            TransferFT(
                self.contract_account,
                self.account,
                receiver,
                how_much=1
                )
            )
        logger.info(f"FT TRANSFER {self.id} DONE")

    def on_start(self):
        super().on_start()

        ft_account = self.contract_account 

        logger.info(f"starting user {self.id} init")
        # self.send_tx(TransferNear(ft_account, self.account_id, 20.0))
        # logger.info(f"user {self.id} TransferNear done")
        self.send_tx(InitFTAccount(ft_account, self.account))
        logger.info(f"user {self.id} InitFTAccount done")
        self.send_tx(TransferFT(ft_account, ft_account, self.account_id, how_much=1E8))
        logger.info(f"user {self.id} TransferFT done")

        logger.info(f"user {self.id} ready")

def send_transaction(node, tx):
    while True:
        block_hash = base58.b58decode(node.get_latest_block().hash)
        signed_tx = tx.finish(block_hash)
        tx_result = node.send_tx_and_wait(signed_tx, timeout=DEFAULT_TRANSACTION_TTL_SECONDS)
        success = "error" not in tx_result
        if success:
            logger.info(f"SUCCESS {tx.transaction_id} (for no account) is successful: {tx_result}")
            return True, tx_result
        elif "UNKNOWN_TRANSACTION" in tx_result:
            logger.debug(
                f"transaction {tx.transaction_id} (for no account) timed out / failed to be accepted"
            )
        else:
            logger.warn(
                f"transaction {tx.transaction_id} (for no account) is not successful: {tx_result}"
            )
        logger.warning(f"transaction {tx.transaction_id} expired, submitting a new one!")


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    args = environment.parsed_options
    contract_code = args.fungible_token_wasm

    funding_key = key.Key.from_json_file(environment.parsed_options.contract_key)
    funding_account = Account(funding_key)
    # TODO; more than one contract
    contract_key = key.Key.from_random(f"ft.{funding_key.account_id}")
    ft_account = Account(contract_key)

    global FUNDING_ACCOUNT
    global FT_ACCOUNT
    FUNDING_ACCOUNT = funding_account
    FT_ACCOUNT = ft_account

    node = cluster.RpcNode("127.0.0.1", "3040") # TODO
    send_transaction(node, CreateSubAccount(funding_account, ft_account.key, balance=50000.0))
    ft_account.refresh_nonce(node)
    logger.info("INIT Deploying")
    send_transaction(node, Deploy(ft_account, contract_code, "FT"))
    logger.info("INIT FT init")
    send_transaction(node, InitFT(ft_account))
    logger.info("INIT DONE")

    
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--fungible-token-wasm", type=str, required=True, help="Path to the compiled Fungible Token contract")
    parser.add_argument("--contract-key", required=True, help= "account to deploy contract to and use as source of NEAR for account creation")

if __name__ == "__main__":
    main()
