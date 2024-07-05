import logging
from concurrent import futures
import random
import string
import sys
import pathlib
import typing
from locust import events, runners
import common.base as base

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, INIT_DONE


class MintInscription(FunctionCall):

    def __init__(self,
                 contract_account_id: str,
                 sender: Account,
                 tick,
                 amt):
        # Attach exactly 1 yoctoNEAR according to NEP-141 to avoid calls from restricted access keys
        super().__init__(sender, contract_account_id, "inscribe", balance=0)
        self.sender = sender
        self.tick = tick
        self.amt = amt

    def args(self) -> dict:
        return {
            "p": "nrc-20",
            "op": "mint",
            "tick": self.tick,
            "amt": str(int(self.amt))
        }

    def sender_account(self) -> Account:
        return self.sender

@events.init.add_listener
def on_locust_init(environment, **kwargs):
    base.INIT_DONE.wait()
    # `master_funding_account` is the same on all runners, allowing to share a
    # single instance of inscription contract.
    funding_account = environment.master_funding_account
    environment.inscription_account_id = f"inscription.{funding_account.key.account_id}"

    # Only create inscription contract on master.
    if isinstance(environment.runner, runners.WorkerRunner):
        return

    node = base.NearNodeProxy(environment)
    funding_account = base.NearUser.funding_account
    funding_account.refresh_nonce(node.node)

    account = base.Account(
        key.Key.from_seed_testonly(environment.inscription_account_id))
    node.prepare_account(account, funding_account, 50000,
                         "create contract account")
    node.send_tx_retry(
        base.Deploy(
            account,
            environment.parsed_options.inscription_wasm,
            "inscription",
        ), "deploy inscription contract")


# Inscription specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--inscription-wasm",
        default="res/inscription.wasm",
        help="Path to the compiled inscription contract",
    )
