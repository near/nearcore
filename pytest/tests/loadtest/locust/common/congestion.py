import json
import pathlib
import sys

from locust import events, runners

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / "lib"))

import account
import common.base as base
import key
import transaction


class ComputeSha256(base.FunctionCall):
    """Transaction with a large input size."""

    def __init__(
        self,
        contract_account_id: str,
        sender: base.Account,
        size_bytes: int,
    ):
        super().__init__(sender, contract_account_id, "ext_sha256")
        self.contract_account_id = contract_account_id
        self.sender = sender
        self.size_bytes = size_bytes

    def args(self) -> dict:
        return ["a" * self.size_bytes]

    def sender_account(self) -> base.Account:
        return self.sender


class ComputeSum(base.Transaction):
    """Large computation that consumes a specified amount of gas."""

    def __init__(
        self,
        contract_account_id: str,
        sender: base.Account,
        usage_tgas: int,
    ):
        super().__init__()
        self.contract_account_id = contract_account_id
        self.sender = sender
        self.usage_tgas = usage_tgas

    def sign_and_serialize(self, block_hash) -> bytes:
        return transaction.sign_function_call_tx(
            self.sender.key,
            self.contract_account_id,
            "sum_n",
            # 1000000 is around 12 TGas.
            ((1000000 * self.usage_tgas) // 12).to_bytes(8, byteorder="little"),
            300 * account.TGAS,
            0,
            self.sender.use_nonce(),
            block_hash,
        )

    def sender_account(self) -> base.Account:
        return self.sender


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    base.INIT_DONE.wait()
    # `master_funding_account` is the same on all runners, allowing to share a
    # single instance of congestion contract.
    funding_account = environment.master_funding_account
    environment.congestion_account_id = f"congestion.{funding_account.key.account_id}"

    # Only create congestion contract on master.
    if isinstance(environment.runner, runners.WorkerRunner):
        return

    node = base.NearNodeProxy(environment)
    funding_account = base.NearUser.funding_account
    funding_account.refresh_nonce(node.node)

    account = base.Account(
        key.Key.from_seed_testonly(environment.congestion_account_id))
    node.prepare_account(account, funding_account, 50000,
                         "create contract account")
    node.send_tx_retry(
        base.Deploy(
            account,
            environment.parsed_options.congestion_wasm,
            "Congestion",
        ), "deploy congestion contract")


# Congestion specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--congestion-wasm",
        default="res/congestion.wasm",
        help="Path to the compiled congestion contract",
    )
