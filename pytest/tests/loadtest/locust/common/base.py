from datetime import datetime, timedelta
from locust import User, events, runners, FastHttpUser
from locust.contrib.fasthttp import FastHttpSession
from retrying import retry
import abc
import base64
import json
import base58
import ctypes
import logging
import multiprocessing
import pathlib
import geventhttpclient as requests
import sys
import threading
import time
import typing
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from account import TGAS
from common.sharding import AccountGenerator
from configured_logger import new_logger
import cluster
import key
import mocknet_helpers
import transaction
import utils

DEFAULT_TRANSACTION_TTL = timedelta(minutes=30)
logger = new_logger(level=logging.WARN)

INIT_DONE = threading.Event()


def is_key_error(exception):
    return isinstance(exception, KeyError)


def is_tx_unknown_error(exception):
    return isinstance(exception, TxUnknownError)


class Account:

    def __init__(self, key):
        self.key = key
        self.current_nonce = multiprocessing.Value(ctypes.c_ulong, 0)

    # Race condition: maybe the account was created but the RPC interface
    # doesn't display it yet, which returns an empty result and raises a
    # `KeyError`.
    # (not quite sure how this happens but it did happen to me on localnet)
    @retry(wait_exponential_multiplier=500,
           wait_exponential_max=10000,
           stop_max_attempt_number=5,
           retry_on_exception=is_key_error)
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

    def fast_forward_nonce(self, ak_nonce):
        with self.current_nonce.get_lock():
            self.current_nonce.value = max(self.current_nonce.value,
                                           ak_nonce + 1)


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

    @abc.abstractmethod
    def sign(self, block_hash) -> transaction.SignedTransaction:
        """
        Each transaction class is supposed to define this method to create and return the signed
        transaction. Transaction needs to be serialized before being sent.
        """

    @abc.abstractmethod
    def sender_account(self) -> Account:
        """
        Account of the sender that signs the tx, which must be known to map
        the tx-result request to the right shard.
        """


class FunctionCall(Transaction):

    def __init__(self,
                 sender: Account,
                 receiver_id: str,
                 method: str,
                 balance: int = 0):
        super().__init__()
        self.sender = sender
        self.receiver_id = receiver_id
        self.method = method
        # defensive cast to avoid serialization bugs when float balance is
        # provided despite type hint
        self.balance = int(balance)

    @abc.abstractmethod
    def args(self) -> typing.Union[dict, typing.List[dict]]:
        """
        Function call arguments to be serialized and sent with the call.
        Return a single dict for `FunctionCall` but a list of dict for `MultiFunctionCall`.
        """

    def sign(self, block_hash) -> transaction.SignedTransaction:
        return transaction.sign_function_call_transaction(
            self.sender.key, self.receiver_id, self.method,
            json.dumps(self.args()).encode('utf-8'), 300 * TGAS, self.balance,
            self.sender.use_nonce(), block_hash)

    def sender_account(self) -> Account:
        return self.sender


class MultiFunctionCall(FunctionCall):
    """
    Batches multiple function calls into a single transaction.
    """

    def __init__(self,
                 sender: Account,
                 receiver_id: str,
                 method: str,
                 balance: int = 0):
        super().__init__(sender, receiver_id, method, balance=balance)

    def sign(self, block_hash) -> transaction.SignedTransaction:
        all_args = self.args()
        gas = 300 * TGAS // len(all_args)

        def create_action(args):
            return transaction.create_function_call_action(
                self.method,
                json.dumps(args).encode('utf-8'), gas, int(self.balance))

        actions = [create_action(args) for args in all_args]
        return transaction.sign_transaction(self.receiver_id,
                                            self.sender.use_nonce(), actions,
                                            block_hash,
                                            self.sender.key.account_id,
                                            self.sender.key.decoded_pk(),
                                            self.sender.key.decoded_sk())


class Deploy(Transaction):

    def __init__(self, account, contract, name):
        super().__init__()
        self.account = account
        self.contract = contract
        self.name = name

    def sign(self, block_hash) -> transaction.SignedTransaction:
        account = self.account
        logger.info(f"deploying {self.name} to {account.key.account_id}")
        wasm_binary = utils.load_binary_file(self.contract)
        return transaction.sign_deploy_contract_transaction(
            account.key, wasm_binary, account.use_nonce(), block_hash)

    def sender_account(self) -> Account:
        return self.account


class CreateSubAccount(Transaction):

    def __init__(self, sender, sub_key, balance: float = 50):
        super().__init__()
        self.sender = sender
        self.sub_key = sub_key
        self.balance = balance

    def sign(self, block_hash) -> transaction.SignedTransaction:
        sender = self.sender
        sub = self.sub_key
        logger.debug(f"creating {sub.account_id}")
        return transaction.sign_create_account_with_full_access_key_and_balance_transaction(
            sender.key, sub.account_id, sub, int(self.balance * 1E24),
            sender.use_nonce(), block_hash)

    def sender_account(self) -> Account:
        return self.sender


class AddFullAccessKey(Transaction):

    def __init__(self, parent: Account, new_key: key.Key):
        super().__init__()
        self.sender = parent
        self.new_key = new_key

    def sign(self, block_hash) -> transaction.SignedTransaction:
        action = transaction.create_full_access_key_action(
            self.new_key.decoded_pk())
        return transaction.sign_transaction(self.sender.key.account_id,
                                            self.sender.use_nonce(), [action],
                                            block_hash,
                                            self.sender.key.account_id,
                                            self.sender.key.decoded_pk(),
                                            self.sender.key.decoded_sk())

    def sender_account(self) -> Account:
        return self.sender


class NearNodeProxy:
    """
    Wrapper around a RPC node connection that tracks requests on locust.
    """

    def __init__(self, environment, user=None):
        self.request_event = environment.events.request
        [url, port] = environment.host.rsplit(":", 1)
        self.session = FastHttpSession(environment,
                                       base_url="http://%s:%s" % (url, port),
                                       user=user,
                                       connection_timeout=6.0,
                                       network_timeout=9.0,
                                       max_retries=3)
        self.node = cluster.RpcNode(url, port, session=self.session)

    def send_tx_retry(self, tx: Transaction, locust_name) -> dict:
        """
        Send a transaction and retry until it succeeds
        
        This method retries no matter the kind of error, but it tries to be
        smart about what to do depending on the error.
        
        Expected error: UnknownTransactionError means TX has not been executed yet.
        Expected error: InvalidNonceError means we are using an outdated nonce.
        Other errors: Probably bugs in the test setup (e.g. invalid signer).
        """
        while True:
            meta = self.send_tx(tx, locust_name)
            error = meta["exception"]
            if error is None:
                return meta
            elif isinstance(error, InvalidNonceError):
                logger.debug(
                    f"{error} for {tx.sender_account().key.account_id}, updating nonce and retrying"
                )
                tx.sender_account().fast_forward_nonce(error.ak_nonce)
            else:
                logger.warn(
                    f"transaction {tx.transaction_id} failed: {error}, retrying in 0.25s"
                )
                time.sleep(0.25)

    def send_tx(self, tx: Transaction, locust_name: str) -> dict:
        """
        Send a transaction and return the result, no retry attempted.
        """
        block_hash = self.final_block_hash()
        signed_tx = tx.sign(block_hash)
        serialized_tx = transaction.serialize_transaction(signed_tx)

        meta = self.new_locust_metadata(locust_name)
        start_perf_counter = time.perf_counter()

        try:
            try:
                # To get proper errors on invalid transaction, we need to wait at least for
                # optimistic execution
                result = self.post_json(
                    "send_tx", {
                        "signed_tx_base64":
                            base64.b64encode(serialized_tx).decode('utf8'),
                        "wait_until":
                            "EXECUTED_OPTIMISTIC"
                    })

                if hasattr(result, 'json'):
                    evaluate_rpc_result(result.json())
                else:
                    try:
                        result.raise_for_status()
                    except Exception as e:
                        raise RpcError(details=e)

            except TxUnknownError as err:
                # This means we time out in one way or another.
                # In that case, the stateless transaction validation was
                # successful, we can now use async API without missing errors.
                submit_raw_response = self.post_json(
                    "send_tx", {
                        "signed_tx_base64":
                            base64.b64encode(serialized_tx).decode('utf8'),
                        "wait_until":
                            "NONE"
                    })
                meta["response_length"] = len(submit_raw_response.text)
                submit_response = json.loads(submit_raw_response.text)
                if not "result" in submit_response:
                    meta["exception"] = RpcError(
                        message="Failed to submit transaction",
                        details=submit_response)
                    meta["response"] = submit_response.content
                else:
                    tx.transaction_id = signed_tx.id
                    # using retrying lib here to poll until a response is ready
                    self.poll_tx_result(meta, tx)
        except NearError as err:
            logging.warn(f"marking an error {err.message}, {err.details}")
            meta["exception"] = err

        meta["response_time"] = (time.perf_counter() -
                                 start_perf_counter) * 1000

        # Track request + response in Locust
        self.request_event.fire(**meta)
        return meta

    def send_tx_async(self, tx: Transaction, locust_name: str) -> dict:
        """
        Send a transaction, but don't wait until it completes.
        """
        block_hash = self.final_block_hash()
        signed_tx = tx.sign(block_hash)
        serialized_tx = transaction.serialize_transaction(signed_tx)

        meta = self.new_locust_metadata(locust_name)
        start_perf_counter = time.perf_counter()

        try:
            submit_raw_response = self.post_json(
                "send_tx", {
                    "signed_tx_base64":
                        base64.b64encode(serialized_tx).decode('utf8'),
                    "wait_until":
                        "NONE"
                })
            meta["response_length"] = len(submit_raw_response.text)
            submit_response = json.loads(submit_raw_response.text)
            if not "result" in submit_response:
                meta["exception"] = RpcError(
                    message="Failed to submit transaction",
                    details=submit_response)
                meta["response"] = submit_response.content
        except NearError as err:
            logging.warn(f"marking an error {err.message}, {err.details}")
            meta["exception"] = err

        meta["response_time"] = (time.perf_counter() -
                                 start_perf_counter) * 1000

        # Track request + response in Locust
        self.request_event.fire(**meta)
        return meta

    def final_block_hash(self):
        return base58.b58decode(
            self.node.get_final_block()['result']['header']['hash'])

    def new_locust_metadata(self, locust_name: str):
        return {
            "request_type": "near-rpc",
            "name": locust_name,
            "start_time": time.time(),
            "response_time": 0,  # overwritten later  with end-to-end time
            "response_length": 0,  # overwritten later
            "response": None,  # overwritten later
            "context": {},  # not used  right now
            "exception": None,  # maybe overwritten later
        }

    def post_json(self, method: str, params: typing.Dict[str, str]):
        j = {
            "method": method,
            "params": params,
            "id": "dontcare",
            "jsonrpc": "2.0"
        }
        return self.session.post(url="/", json=j)

    @retry(wait_fixed=500,
           stop_max_delay=DEFAULT_TRANSACTION_TTL / timedelta(milliseconds=1),
           retry_on_exception=is_tx_unknown_error)
    def poll_tx_result(self, meta: dict, tx: Transaction):
        params = {
            "tx_hash": tx.transaction_id,
            "sender_account_id": tx.sender_account().key.account_id
        }
        # poll for tx result, using "EXPERIMENTAL_tx_status" which waits for
        # all receipts to finish rather than just the first one, as "tx" would do
        result_response = self.post_json("EXPERIMENTAL_tx_status", params)
        # very verbose, but very useful to see what's happening when things are stuck
        logger.debug(
            f"polling, got: {result_response.status_code} {json.loads(result_response.text)}"
        )

        try:
            meta["response"] = evaluate_rpc_result(
                json.loads(result_response.text))
        except:
            # Store raw response to improve error-reporting.
            meta["response"] = result_response.content
            raise

    def account_exists(self, account_id: str) -> bool:
        return "error" not in self.node.get_account(account_id, do_assert=False)

    def prepare_account(self, account: Account, parent: Account, balance: float,
                        msg: str) -> bool:
        """
        Creates the account if it doesn't exist and refreshes the nonce.
        """
        exists = self.account_exists(account.key.account_id)
        if not exists:
            self.send_tx_retry(
                CreateSubAccount(parent, account.key, balance=balance), msg)
        account.refresh_nonce(self.node)
        return exists

    def prepare_accounts(self,
                         accounts: typing.List[Account],
                         parent: Account,
                         balance: float,
                         msg: str,
                         timeout: timedelta = timedelta(minutes=3)):
        """
        Creates accounts if they don't exist and refreshes their nonce.
        Accounts must share the parent account.
        
        This implementation attempts on-chain parallelization, hence it should
        be faster than calling `prepare_account` in a loop.
        
        Note that error-handling in this variant isn't quite as smooth. Errors
        that are only reported by the sync API of RPC nodes will not be caught
        here. Instead, we do a best-effort retry and stop after a fixed timeout.
        """

        # To avoid blocking, each account goes though a FSM independently.
        #
        # FSM outline:
        #
        #    [INIT] -----------------(account exists already)-------------
        #       |                                                        |
        #       V                                                        V
        #  [TO_CREATE] --(post tx)--> [INFLIGHT] --(poll result)--> [TO_REFRESH]
        #   ^                    |                                       |
        #   |                    |                                       |
        #   |--(fail to submit)<--                                   (refresh)
        #                                                                |
        #                                                                V
        #                                                              [DONE]
        #
        to_create: typing.List[Account] = []
        inflight: typing.List[Transaction, dict, Account] = []
        to_refresh: typing.List[Account] = []

        for account in accounts:
            if self.account_exists(account.key.account_id):
                to_refresh.append(account)
            else:
                to_create.append(account)

        block_hash = self.final_block_hash()
        start = datetime.now()
        while len(to_create) + len(to_refresh) + len(inflight) > 0:
            logger.info(
                f"preparing {len(accounts)} accounts, {len(to_create)} to create, {len(to_refresh)} to refresh, {len(inflight)} inflight"
            )
            if start + timeout < datetime.now():
                raise SystemExit("Account preparation timed out")
            try_again = []
            for account in to_create:
                meta = self.new_locust_metadata(msg)
                tx = CreateSubAccount(parent, account.key, balance=balance)
                signed_tx = tx.sign(block_hash)
                serialized_tx = transaction.serialize_transaction(signed_tx)
                submit_raw_response = self.post_json(
                    "send_tx", {
                        "signed_tx_base64":
                            base64.b64encode(serialized_tx).decode('utf8'),
                        "wait_until":
                            "NONE"
                    })
                meta["response_length"] = len(submit_raw_response.text)
                submit_response = json.loads(submit_raw_response.text)
                if not "result" in submit_response:
                    # something failed, let's not block, just try again later
                    logger.debug(
                        f"couldn't submit account creation TX, got {submit_raw_response.text}"
                    )
                    try_again.append(account)
                else:
                    tx.transaction_id = signed_tx.id
                    inflight.append((tx, meta, account))
            to_create = try_again

            # while requests are processed on-chain, refresh nonces for existing accounts
            for account in to_refresh:
                account.refresh_nonce(self.node)
            to_refresh.clear()

            # poll all pending requests
            for tx, meta, account in inflight:
                # Using retrying lib here to poll until a response is ready.
                # This is blocking on a single request, but we expect requests
                # to be processed in order so it shouldn't matter.
                self.poll_tx_result(meta, tx)
                meta["response_time"] = (time.time() -
                                         meta["start_time"]) * 1000
                to_refresh.append(account)
                # Locust tracking
                self.request_event.fire(**meta)
            inflight.clear()

        logger.info(f"done preparing {len(accounts)} accounts")


class NearUser(FastHttpUser):
    abstract = True
    id_counter = 0
    INIT_BALANCE = 100.0
    funding_account: Account

    @classmethod
    def get_next_id(cls):
        cls.id_counter += 1
        return cls.id_counter

    @classmethod
    def generate_account_id(cls, account_generator, id) -> str:
        return account_generator.random_account_id(
            cls.funding_account.key.account_id, f'_user{id}')

    def __init__(self, environment):
        super().__init__(environment)
        assert self.host is not None, "Near user requires the RPC node address"
        self.node = NearNodeProxy(environment, self)
        self.id = NearUser.get_next_id()
        self.user_suffix = f"{self.id}_run{environment.parsed_options.run_id}"
        self.account_generator = environment.account_generator

    def on_start(self):
        """
        Called once per user, creating the account on chain
        """
        INIT_DONE.wait()
        self.account_id = NearUser.generate_account_id(self.account_generator,
                                                       self.user_suffix)
        self.account = Account(key.Key.from_random(self.account_id))
        if not self.node.account_exists(self.account_id):
            self.send_tx_retry(
                CreateSubAccount(NearUser.funding_account,
                                 self.account.key,
                                 balance=NearUser.INIT_BALANCE))
        self.account.refresh_nonce(self.node.node)

    def send_tx(self, tx: Transaction, locust_name="generic send_tx"):
        """
        Send a transaction and return the result, no retry attempted.
        """
        return self.node.send_tx(tx, locust_name)["response"]

    def send_tx_async(self,
                      tx: Transaction,
                      locust_name="generic send_tx_async"):
        """
        Send a transaction, but don't wait until it completes.
        """
        return self.node.send_tx_async(tx, locust_name)["response"]

    def send_tx_retry(self,
                      tx: Transaction,
                      locust_name="generic send_tx_retry"):
        """
        Send a transaction and retry until it succeeds
        """
        return self.node.send_tx_retry(tx, locust_name=locust_name)["response"]


class NearError(Exception):

    def __init__(self, message, details):
        """
        The `message` is used in locust to aggregate errors and is also displayed in the UI.
        The `details` are logged as additional information in the console.
        """
        self.message = message
        self.details = details
        super().__init__(message)


class RpcError(NearError):

    def __init__(self, message="RPC returned an error", details=None):
        super().__init__(message, details)


class TxUnknownError(RpcError):

    def __init__(
        self,
        message="RPC does not know the result of this TX, probably it is not executed yet"
    ):
        super().__init__(message=message)


class InvalidNonceError(RpcError):

    def __init__(
        self,
        used_nonce,
        ak_nonce,
    ):
        super().__init__(
            message="Nonce too small",
            details=
            f"Tried to use nonce {used_nonce} but access key nonce is {ak_nonce}"
        )
        self.ak_nonce = ak_nonce


class TxError(NearError):

    def __init__(self,
                 status,
                 message="Transaction to receipt conversion failed"):
        super().__init__(message, status)


class ReceiptError(NearError):

    def __init__(self, status, receipt_id, message="Receipt execution failed"):
        super().__init__(message, f"id={receipt_id} {status}")


class SmartContractPanic(ReceiptError):

    def __init__(self, status, receipt_id, message="Smart contract panicked"):
        super().__init__(status, receipt_id, message)


class FunctionExecutionError(ReceiptError):

    def __init__(self,
                 status,
                 receipt_id,
                 message="Smart contract function execution failed"):
        super().__init__(status, receipt_id, message)


def evaluate_rpc_result(rpc_result):
    """
    Take the json RPC response and translate it into success
    and failure cases. Failures are raised as exceptions.
    """
    if "error" in rpc_result:
        err_name = rpc_result["error"]["cause"]["name"]
        # The sync API returns "UNKNOWN_TRANSACTION" after a timeout.
        # The async API returns "TIMEOUT_ERROR" if the tx was not accepted in the chain after 10s.
        # In either case, the identical transaction should be retried.
        if err_name in ["UNKNOWN_TRANSACTION", "TIMEOUT_ERROR"]:
            raise TxUnknownError(err_name)
        # When reusing keys across test runs, the nonce is higher than expected.
        elif err_name == "INVALID_TRANSACTION":
            err_description = rpc_result["error"]["data"]["TxExecutionError"][
                "InvalidTxError"]
            if "InvalidNonce" in err_description:
                raise InvalidNonceError(
                    err_description["InvalidNonce"]["tx_nonce"],
                    err_description["InvalidNonce"]["ak_nonce"])
        raise RpcError(details=rpc_result["error"])

    result = rpc_result["result"]
    transaction_outcome = result["transaction_outcome"]
    if not "SuccessReceiptId" in transaction_outcome["outcome"]["status"]:
        raise TxError(transaction_outcome["outcome"]["status"])

    receipt_outcomes = result["receipts_outcome"]
    for receipt in receipt_outcomes:
        # For each receipt, we get
        # `{ "outcome": { ..., "status": { <ExecutionStatusView>: "..." } } }`
        # and the key for `ExecutionStatusView` tells us whether it was successful
        status = list(receipt["outcome"]["status"].keys())[0]

        if status == "Unknown":
            raise ReceiptError(receipt["outcome"],
                               receipt["id"],
                               message="Unknown receipt result")
        if status == "Failure":
            failure = receipt["outcome"]["status"]["Failure"]
            panic_msg = as_smart_contract_panic_message(failure)
            if panic_msg:
                raise SmartContractPanic(receipt["outcome"],
                                         receipt["id"],
                                         message=panic_msg)
            exec_failed = as_execution_error(failure)
            if exec_failed:
                raise FunctionExecutionError(receipt["outcome"],
                                             receipt["id"],
                                             message=exec_failed)
            raise ReceiptError(receipt["outcome"], receipt["id"])
        if not status in ["SuccessReceiptId", "SuccessValue"]:
            raise ReceiptError(receipt["outcome"],
                               receipt["id"],
                               message="Unexpected status")

    return result


def as_action_error(failure: dict) -> typing.Optional[dict]:
    return failure.get("ActionError", None)


def as_function_call_error(failure: dict) -> typing.Optional[dict]:
    action_error = as_action_error(failure)
    if action_error and "FunctionCallError" in action_error["kind"]:
        return action_error["kind"]["FunctionCallError"]
    return None


def as_execution_error(failure: dict) -> typing.Optional[dict]:
    function_call_error = as_function_call_error(failure)
    if function_call_error and "ExecutionError" in function_call_error:
        return function_call_error["ExecutionError"]
    return None


def as_smart_contract_panic_message(failure: dict) -> typing.Optional[str]:
    execution_error = as_execution_error(failure)
    known_prefix = "Smart contract panicked: "
    if execution_error and execution_error.startswith(known_prefix):
        return execution_error[len(known_prefix):]
    return None


def init_account_generator(parsed_options):
    if parsed_options.shard_layout_file is not None:
        with open(parsed_options.shard_layout_file, 'r') as f:
            shard_layout = json.load(f)
        shard_layout_version = "V1" if "V1" in shard_layout else "V0"
    elif parsed_options.shard_layout_chain_id is not None:
        if parsed_options.shard_layout_chain_id not in ['mainnet', 'testnet']:
            sys.exit(
                f'unexpected --shard-layout-chain-id: {parsed_options.shard_layout_chain_id}'
            )

        shard_layout = {
            "V1": {
                "fixed_shards": [],
                "boundary_accounts": [
                    "aurora", "aurora-0", "kkuuue2akv_1630967379.near"
                ],
                "shards_split_map": [[0, 1, 2, 3]],
                "to_parent_shard_map": [0, 0, 0, 0],
                "version": 1
            },
            "V2": {
                "fixed_shards": [],
                "boundary_accounts": [
                    "aurora", "aurora-0", "kkuuue2akv_1630967379.near",
                    "tge-lockup.sweat"
                ],
                "shards_split_map": [[0, 1, 2, 3, 4]],
                "to_parent_shard_map": [0, 0, 0, 0, 0],
                "version": 2
            },
            "V3": {
                "fixed_shards": [],
                "boundary_accounts": [
                    "aurora",
                    "aurora-0",
                    "game.hot.tg",
                    "kkuuue2akv_1630967379.near",
                    "tge-lockup.sweat",
                ],
                "shards_split_map": [[0, 1, 2, 3, 4, 5]],
                "to_parent_shard_map": [0, 0, 0, 0, 0, 0],
                "version": 3
            }
        }
        shard_layout_version = parsed_options.shard_layout_version.upper()
    else:
        shard_layout = {
            "V0": {
                "num_shards": 1,
                "version": 0,
            },
        }
        shard_layout_version = "V0"
    return AccountGenerator(shard_layout, shard_layout_version)


# called once per process before user initialization
def do_on_locust_init(environment):
    node = NearNodeProxy(environment)

    master_funding_key = key.Key.from_json_file(
        environment.parsed_options.funding_key)
    master_funding_account = Account(master_funding_key)

    if not node.account_exists(master_funding_account.key.account_id):
        raise SystemExit(
            f"account {master_funding_account.key.account_id} of the provided master funding key does not exist"
        )
    master_funding_account.refresh_nonce(node.node)

    environment.account_generator = init_account_generator(
        environment.parsed_options)
    funding_account = None
    # every worker needs a funding account to create its users, eagerly create them in the master
    if isinstance(environment.runner, runners.MasterRunner):
        num_funding_accounts = environment.parsed_options.max_workers
        funding_balance = 10000 * NearUser.INIT_BALANCE

        def create_account(id):
            account_id = f"funds_worker_{id}.{master_funding_account.key.account_id}"
            return Account(key.Key.from_seed_testonly(account_id))

        funding_accounts = [
            create_account(id) for id in range(num_funding_accounts)
        ]
        node.prepare_accounts(funding_accounts, master_funding_account,
                              funding_balance, "create funding account")
        funding_account = master_funding_account
    elif isinstance(environment.runner, runners.WorkerRunner):
        worker_id = environment.runner.worker_index
        worker_account_id = f"funds_worker_{worker_id}.{master_funding_account.key.account_id}"
        worker_key = key.Key.from_seed_testonly(worker_account_id)
        funding_account = Account(worker_key)
        funding_account.refresh_nonce(node.node)
    elif isinstance(environment.runner, runners.LocalRunner):
        funding_account = master_funding_account
    else:
        raise SystemExit(
            f"unexpected runner class {environment.runner.__class__.__name__}")

    NearUser.funding_account = funding_account
    environment.master_funding_account = master_funding_account


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    do_on_locust_init(environment)
    INIT_DONE.set()


# Add custom CLI args here, will be available in `environment.parsed_options`
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--funding-key",
        required=True,
        help="account to use as source of NEAR for account creation")
    parser.add_argument(
        "--max-workers",
        type=int,
        required=False,
        default=16,
        help="How many funding accounts to generate for workers")
    parser.add_argument(
        "--shard-layout-file",
        required=False,
        help="file containing a shard layout JSON object for the target chain")
    parser.add_argument(
        "--shard-layout-chain-id",
        required=False,
        help=
        "chain ID whose shard layout we should consult when generating account IDs. Convenience option to avoid using --shard-layout-file for mainnet and testnet"
    )
    parser.add_argument(
        "--shard-layout-version",
        required=False,
        help=
        "Version of the shard layout. Only works with --shard-layout-chain-id. Should be one of V0, V1, V2, or V3."
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Unique index to append to static account ids. "
        "Change between runs if you need a new state. Keep at default if you want to reuse the old state"
    )


class TestEvaluateRpcResult(unittest.TestCase):

    def test_smart_contract_panic(self):
        input = """{
          "result": {
            "transaction_outcome": { "outcome": {"status": { "SuccessReceiptId": "" } } },
            "receipts_outcome": [ {
              "id": "J3EVpgJXgLQ5f33ammArtewYBAg3KmDgVf47HtapBtua",
              "outcome": {
                "logs": [],
                "receipt_ids": [
                  "HxL55zV91tEgpPKg8QPkoWo53Ue1x9yhfRQTgdfQ11mc",
                  "467VVuaNz9igj74Zs9wFpeYmtfpRorbZugJKDHymCN1Q"
                ],
                "gas_burnt": 2658479078129,
                "tokens_burnt": "265847907812900000000",
                "executor_id": "vy0zxd_ft.funds_worker_3.node0",
                "status": {
                  "Failure": {
                    "ActionError": {
                      "index": 0,
                      "kind": {
                        "FunctionCallError": {
                          "ExecutionError": "Smart contract panicked: The account doesnt have enough balance"
                        }
                      }
                    }
                  }
                },
                "metadata": {
                  "version": 3,
                  "gas_profile": []
                }
              }
            } ]
          }
        }"""
        self.assertRaises(SmartContractPanic, evaluate_rpc_result,
                          json.loads(input))
