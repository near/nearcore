from datetime import timedelta
from locust import User, events, runners
from retrying import retry
import abc
import base64
import json
import base58
import ctypes
import logging
import multiprocessing
import pathlib
import requests
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
    def sign_and_serialize(self, block_hash) -> bytes:
        """
        Each transaction class is supposed to define this method to serialize and
        sign the transaction and return the raw message to be sent.
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
    def args(self) -> dict:
        """
        Function call arguments to be serialized and sent with the call.
        """

    def sign_and_serialize(self, block_hash) -> bytes:
        return transaction.sign_function_call_tx(
            self.sender.key, self.receiver_id, self.method,
            json.dumps(self.args()).encode('utf-8'), 300 * TGAS, self.balance,
            self.sender.use_nonce(), block_hash)

    def sender_account(self) -> Account:
        return self.sender


class Deploy(Transaction):

    def __init__(self, account, contract, name):
        super().__init__()
        self.account = account
        self.contract = contract
        self.name = name

    def sign_and_serialize(self, block_hash) -> bytes:
        account = self.account
        logger.info(f"deploying {self.name} to {account.key.account_id}")
        wasm_binary = utils.load_binary_file(self.contract)
        return transaction.sign_deploy_contract_tx(account.key, wasm_binary,
                                                   account.use_nonce(),
                                                   block_hash)

    def sender_account(self) -> Account:
        return self.account


class CreateSubAccount(Transaction):

    def __init__(self, sender, sub_key, balance: int = 50):
        super().__init__()
        self.sender = sender
        self.sub_key = sub_key
        self.balance = balance

    def sign_and_serialize(self, block_hash) -> bytes:
        sender = self.sender
        sub = self.sub_key
        logger.debug(f"creating {sub.account_id}")
        return transaction.sign_create_account_with_full_access_key_and_balance_tx(
            sender.key, sub.account_id, sub, int(self.balance * 1E24),
            sender.use_nonce(), block_hash)

    def sender_account(self) -> Account:
        return self.sender


class NearNodeProxy:
    """
    Wrapper around a RPC node connection that tracks requests on locust.
    """

    def __init__(self, environment):
        self.request_event = environment.events.request
        url, port = environment.host.split(":")
        self.node = cluster.RpcNode(url, port)
        self.session = requests.Session()

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

    def send_tx(self, tx: Transaction, locust_name) -> dict:
        """
        Send a transaction and return the result, no retry attempted.
        """
        block_hash = base58.b58decode(self.node.get_latest_block().hash)
        signed_tx = tx.sign_and_serialize(block_hash)

        meta = {
            "request_type": "near-rpc",
            "name": locust_name,
            "start_time": time.time(),
            "response_time": 0,  # overwritten later  with end-to-end time
            "response_length": 0,  # overwritten later
            "response": None,  # overwritten later
            "context": {},  # not used  right now
            "exception": None,  # maybe overwritten later
        }
        start_perf_counter = time.perf_counter()

        try:
            try:
                # To get proper errors on invalid transaction, we need to use sync api first
                result = self.post_json(
                    "broadcast_tx_commit",
                    [base64.b64encode(signed_tx).decode('utf8')])
                evaluate_rpc_result(result.json())
            except TxUnknownError as err:
                # This means we time out in one way or another.
                # In that case, the stateless transaction validation was
                # successful, we can now use async API without missing errors.
                submit_raw_response = self.post_json(
                    "broadcast_tx_async",
                    [base64.b64encode(signed_tx).decode('utf8')])
                meta["response_length"] = len(submit_raw_response.text)
                submit_response = submit_raw_response.json()
                # extract transaction ID from response, it should be "{ "result": "id...." }"
                if not "result" in submit_response:
                    meta["exception"] = RpcError(submit_response,
                                                 message="Didn't get a TX ID")
                    meta["response"] = submit_response.content
                else:
                    tx.transaction_id = submit_response["result"]
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

    def post_json(self, method: str, params: typing.List[str]):
        j = {
            "method": method,
            "params": params,
            "id": "dontcare",
            "jsonrpc": "2.0"
        }
        return self.session.post(url="http://%s:%s" % self.node.rpc_addr(),
                                 json=j)

    @retry(wait_fixed=500,
           stop_max_delay=DEFAULT_TRANSACTION_TTL / timedelta(milliseconds=1),
           retry_on_exception=is_tx_unknown_error)
    def poll_tx_result(self, meta: dict, tx: Transaction):
        params = [tx.transaction_id, tx.sender_account().key.account_id]
        # poll for tx result, using "EXPERIMENTAL_tx_status" which waits for
        # all receipts to finish rather than just the first one, as "tx" would do
        result_response = self.post_json("EXPERIMENTAL_tx_status", params)
        # very verbose, but very useful to see what's happening when things are stuck
        logger.debug(
            f"polling, got: {result_response.status_code} {result_response.json()}"
        )

        try:
            meta["response"] = evaluate_rpc_result(result_response.json())
        except:
            # Store raw response to improve error-reporting.
            meta["response"] = result_response.content
            raise

    def account_exists(self, account_id: str) -> bool:
        return "error" not in self.node.get_account(account_id, do_assert=False)

    def prepare_account(self, account: Account, parent: Account, balance: int,
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


class NearUser(User):
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
        self.node = NearNodeProxy(environment)
        self.id = NearUser.get_next_id()
        user_suffix = f"{self.id}_run{environment.parsed_options.run_id}"
        self.account_id = NearUser.generate_account_id(
            environment.account_generator, user_suffix)

    def on_start(self):
        """
        Called once per user, creating the account on chain
        """
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

    def send_tx_retry(self,
                      tx: Transaction,
                      locust_name="generic send_tx_retry"):
        """
        Send a transaction and retry until it succeeds
        """
        return self.node.send_tx_retry(tx, locust_name=locust_name)["response"]


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


class InvalidNonceError(RpcError):

    def __init__(
        self,
        used_nonce,
        ak_nonce,
    ):
        super().__init__(
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
        raise RpcError(rpc_result["error"])

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
            }
        }
    else:
        shard_layout = {
            "V0": {
                "num_shards": 1,
                "version": 0,
            },
        }

    return AccountGenerator(shard_layout)


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
        # TODO: Create accounts in parallel
        for id in range(num_funding_accounts):
            account_id = f"funds_worker_{id}.{master_funding_account.key.account_id}"
            worker_key = key.Key.from_seed_testonly(account_id)
            node.prepare_account(Account(worker_key), master_funding_account,
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


INIT_DONE = threading.Event()


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
