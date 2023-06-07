import json
import pathlib
import sys

from locust import events, runners

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / "lib"))

import account
import cluster
import common.base as base
import key
import transaction


class LargeTransaction(base.Transaction):
    """Transaction with a large size in bytes."""

    def __init__(
        self,
        contract_account_id: str,
        sender: base.Account,
        size_bytes: int,
    ):
        super().__init__()
        self.contract_account_id = contract_account_id
        self.sender = sender
        self.size_bytes = size_bytes

    def sign_and_serialize(self, block_hash) -> bytes:
        args = {
            "key": "a" * self.size_bytes,
        }
        return transaction.sign_function_call_tx(
            self.sender.key,
            self.contract_account_id,
            "ext_sha256",
            json.dumps(args).encode("utf-8"),
            300 * account.TGAS,
            # Attach exactly 1 yoctoNEAR according to NEP-141 to avoid
            # calls from restricted access keys.
            1,
            self.sender.use_nonce(),
            block_hash,
        )


class LongTransaction(base.Transaction):
    """Transaction with a large size in bytes."""

    def __init__(
        self,
        contract_account_id: str,
        sender: base.Account,
    ):
        super().__init__()
        self.contract_account_id = contract_account_id
        self.sender = sender

    def sign_and_serialize(self, block_hash) -> bytes:
        return transaction.sign_function_call_tx(
            self.sender.key,
            self.contract_account_id,
            "fibonacci",
            [33],
            300 * account.TGAS,
            # Attach exactly 1 yoctoNEAR according to NEP-141 to avoid
            # calls from restricted access keys.
            1,
            self.sender.use_nonce(),
            block_hash,
        )


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if not base.is_tag_active(environment, "congestion"):
        return

    # `master_funding_account` is the same on all runners, allowing to share a
    # single instance of Congestion contract.
    funding_account = environment.master_funding_account
    environment.congestion_account_id = (
        f"congestion.{funding_account.key.account_id}"
    )

    # Create SocialDB account, unless we are a worker, in which case the master already did it
    if isinstance(environment.runner, runners.WorkerRunner):
        return

    contract_key = key.Key.from_random(environment.congestion_account_id)
    account = base.Account(contract_key)

    # Note: These setup requests are not tracked by locust because we use our own http session
    host, port = environment.host.split(":")
    node = cluster.RpcNode(host, port)

    funding_account = base.NearUser.funding_account
    funding_account.refresh_nonce(node)
    base.send_transaction(
        node,
        base.CreateSubAccount(funding_account, account.key, balance=50000.0),
    )
    account.refresh_nonce(node)
    contract_code = environment.parsed_options.congestion_wasm
    base.send_transaction(
        node, base.Deploy(account, contract_code, "Congestion Contract")
    )


# Congestion specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--congestion-wasm",
        type=str,
        required=False,
        help="Path to the compiled congestion contract",
    )
