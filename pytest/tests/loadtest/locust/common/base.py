import base64
import json
import base58
import ctypes
import locust
import logging
import multiprocessing
import pathlib
import random
import requests
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import cluster
import transaction
import key
import mocknet_helpers
import utils

from locust import HttpUser, events
from configured_logger import new_logger

DEFAULT_TRANSACTION_TTL_SECONDS = 20
logger = new_logger(level=logging.WARN)


class Account:

    def __init__(self, key):
        self.key = key
        self.current_nonce = multiprocessing.Value(ctypes.c_ulong, 0)

    def refresh_nonce(self, node):
        with self.current_nonce.get_lock():
            self.current_nonce.value = mocknet_helpers.get_nonce_for_key(
                self.key,
                addr=node.rpc_addr()[0],
                port=node.rpc_addr()[1],
                logger=logger,
            )

    def use_nonce(self):
        with self.current_nonce.get_lock():
            new_nonce = self.current_nonce.value + 1
            self.current_nonce.value = new_nonce
            return new_nonce


class Transaction:
    """
    A transaction future.
    """

    ID = 0

    def __init__(self):
        self.id = Transaction.ID
        Transaction.ID += 1

        # The transaction id hash
        #
        # str if the transaction has been submitted and may eventually conclude.
        # FIXME: this is currently not set in some cases
        self.transaction_id = None

    def sign_and_serialize(self, block_hash):
        """
        Each transaction class is supposed to define this method to serialize and
        sign the transaction and return the raw message to be sent.
        """
        return None


class Deploy(Transaction):

    def __init__(self, account, contract, name):
        super().__init__()
        self.account = account
        self.contract = contract
        self.name = name

    def sign_and_serialize(self, block_hash):
        account = self.account
        logger.info(f"deploying {self.name} to {account.key.account_id}")
        wasm_binary = utils.load_binary_file(self.contract)
        return transaction.sign_deploy_contract_tx(account.key, wasm_binary,
                                                   account.use_nonce(),
                                                   block_hash)


class CreateSubAccount(Transaction):

    def __init__(self, sender, sub_key, balance=50.0):
        super().__init__()
        self.sender = sender
        self.sub_key = sub_key
        self.balance = balance

    def sign_and_serialize(self, block_hash):
        sender = self.sender
        sub = self.sub_key
        logger.debug(f"creating {sub.account_id}")
        return transaction.sign_create_account_with_full_access_key_and_balance_tx(
            sender.key, sub.account_id, sub, int(self.balance * 1E24),
            sender.use_nonce(), block_hash)


class NearUser(HttpUser):
    abstract = True
    id_counter = 0
    # initialized in `on_locust_init`
    funding_account = None

    @classmethod
    def get_next_id(cls):
        cls.id_counter += 1
        return cls.id_counter

    @classmethod
    def account_id(cls, id):
        # Pseudo-random 6-digit prefix to spread the users in the state tree
        prefix = str(hash(str(id)))[-6:]
        return f"{prefix}_user{id}.{cls.funding_account.key.account_id}"

    def __init__(self, environment):
        super().__init__(environment)
        host, port = self.host.split(":")
        self.node = cluster.RpcNode(host, port)
        self.id = NearUser.get_next_id()
        self.account_id = NearUser.account_id(self.id)

    def on_start(self):
        """
        Called once per user, creating the account on chain
        """
        self.account = Account(key.Key.from_random(self.account_id))
        self.send_tx_retry(
            CreateSubAccount(NearUser.funding_account,
                             self.account.key,
                             balance=5000.0))
        self.account.refresh_nonce(self.node)

    def send_tx(self, tx: Transaction):
        """
        Send a transaction and return the result, no retry attempted.
        """
        block_hash = base58.b58decode(self.node.get_latest_block().hash)
        signed_tx = tx.sign_and_serialize(block_hash)

        # doesn't work because it raises on status etc
        # rpc_result = self.node.send_tx_and_wait(signed_tx, timeout=DEFAULT_TRANSACTION_TTL_SECONDS)

        params = [base64.b64encode(signed_tx).decode('utf8')]
        j = {
            "method": "broadcast_tx_commit",
            "params": params,
            "id": "dontcare",
            "jsonrpc": "2.0"
        }

        # with self.node.session.post(
        # This is tracked by locust
        with self.client.post(
                url="http://%s:%s" % self.node.rpc_addr(),
                json=j,
                timeout=DEFAULT_TRANSACTION_TTL_SECONDS,
                catch_response=True,
        ) as response:
            try:
                rpc_result = json.loads(response.content)
                tx_result = evaluate_rpc_result(rpc_result)
                tx.transaction_id = tx_result["transaction_outcome"]["id"]
                logger.debug(
                    f"{tx.transaction_id} for {self.account_id} is successful: {tx_result}"
                )
            except NearError as err:
                logging.warn(f"marking an error {err.message}, {err.details}")
                response.failure(err.message)
        return response

    def send_tx_retry(self, tx: Transaction):
        """
        Send a transaction and retry until it succeeds
        """
        # expected error: UNKNOWN_TRANSACTION means TX has not been executed yet
        # other errors: probably bugs in the test setup (e.g. invalid signer)
        # this method is very simple and just retries no matter the kind of
        # error, as long as it is one defined by us (inherits from NearError)
        while True:
            try:
                result = self.send_tx(tx)
                return result
            except NearError as error:
                logger.warn(
                    f"transaction {tx.transaction_id} failed: {error}, retrying in 0.25s"
                )
                time.sleep(0.25)


def send_transaction(node, tx):
    """
    Send a transaction without a user.
    Retry until it is successful.
    Used for setting up accounts before actual users start their load.
    """
    while True:
        block_hash = base58.b58decode(node.get_latest_block().hash)
        signed_tx = tx.sign_and_serialize(block_hash)
        tx_result = node.send_tx_and_wait(
            signed_tx, timeout=DEFAULT_TRANSACTION_TTL_SECONDS)
        success = "error" not in tx_result
        if success:
            logger.debug(
                f"transaction {tx.transaction_id} (for no account) is successful: {tx_result}"
            )
            return True, tx_result
        elif "UNKNOWN_TRANSACTION" in tx_result:
            logger.debug(
                f"transaction {tx.transaction_id} (for no account) timed out")
        else:
            logger.warn(
                f"transaction {tx.transaction_id} (for no account) is not successful: {tx_result}"
            )
        logger.info(f"re-submitting transaction {tx.transaction_id}")


class NearError(Exception):

    def __init__(self, message, details):
        self.message = message
        self.details = details
        super().__init__(message)


class RpcError(NearError):

    def __init__(self, error, message="RPC returned an error"):
        super().__init__(message, error)


class TxUnknownError(RpcError):

    def __init__(
        self,
        message="RPC does not know the result of this TX, probably it is not executed yet"
    ):
        super().__init__(message)


class TxError(NearError):

    def __init__(self,
                 status,
                 message="Transaction to receipt conversion failed"):
        super().__init__(message, status)


class ReceiptError(NearError):

    def __init__(self, status, receipt_id, message="Receipt execution failed"):
        super().__init__(message, f"id={receipt_id} {status}")


def evaluate_rpc_result(rpc_result):
    """
    Take the json RPC response and translate it into success
    and failure cases. Failures are raised as exceptions.
    """
    if not "result" in rpc_result:
        raise NearError("No result returned", f"Error: {rpc_result}")

    result = rpc_result["result"]

    if "UNKNOWN_TRANSACTION" in result:
        raise TxUnknownError("UNKNOWN_TRANSACTION")
    elif not "transaction_outcome" in result:
        raise RpcError(result)

    transaction_outcome = result["transaction_outcome"]
    if not "SuccessReceiptId" in transaction_outcome["outcome"]["status"]:
        raise TxError(transaction_outcome["outcome"]["status"])

    receipt_outcomes = result["receipts_outcome"]
    for receipt in receipt_outcomes:
        outcome = receipt["outcome"]
        if not "SuccessValue" in outcome["status"]:
            raise ReceiptError(outcome["status"], receipt["id"])
    return result


# called once per process before user initialization
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    funding_key = key.Key.from_json_file(environment.parsed_options.funding_key)
    NearUser.funding_account = Account(funding_key)


# CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--funding-key",
        required=True,
        help="account to use as source of NEAR for account creation")
