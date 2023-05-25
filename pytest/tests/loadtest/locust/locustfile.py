"""
TODO
"""

import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))

import account
import cluster
import common
import key
from configured_logger import new_logger
from locust import between, task, events
from common.base import Account, CreateSubAccount, Deploy, NearUser, send_transaction
from common.ft import InitFT, InitFTAccount, TransferFT

logger = new_logger(level=logging.WARN)

FT_ACCOUNT = None


class FtTransferUser(NearUser):
    wait_time = between(1, 3)  # random pause between transactions
    registered_users = []

    @task
    def ft_transfer(self):
        rng = random.Random()
        receiver = rng.choice(FtTransferUser.registered_users)
        # Sender must be != receiver but maybe there is no other registered user
        # yet, so we just send to the contract account which is registered
        # implicitly from the start
        if receiver == self.account_id:
            receiver = self.contract_account.key.account_id

        self.send_tx(
            TransferFT(self.contract_account,
                       self.account,
                       receiver,
                       how_much=1))

    def on_start(self):
        super().on_start()

        self.contract_account = FT_ACCOUNT

        self.send_tx(InitFTAccount(self.contract_account, self.account))
        self.send_tx(
            TransferFT(self.contract_account,
                       self.contract_account,
                       self.account_id,
                       how_much=1E8))
        logger.debug(
            f"{self.account_id} ready to use FT contract {self.contract_account.key.account_id}"
        )

        FtTransferUser.registered_users.append(self.account_id)


# called once per process before user initialization
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    funding_account = NearUser.funding_account
    contract_code = environment.parsed_options.fungible_token_wasm

    # TODO: more than one FT contract
    contract_key = key.Key.from_random(f"ft.{funding_account.key.account_id}")
    ft_account = Account(contract_key)

    global FT_ACCOUNT
    FT_ACCOUNT = ft_account

    # Note: These setup requests are not tracked by locust because we use our own http session
    host, port = environment.host.split(":")
    node = cluster.RpcNode(host, port)
    send_transaction(
        node, CreateSubAccount(funding_account, ft_account.key,
                               balance=50000.0))
    ft_account.refresh_nonce(node)
    send_transaction(node, Deploy(ft_account, contract_code, "FT"))
    send_transaction(node, InitFT(ft_account))


# Add custom CLI args here, will be available in `environment.parsed_options`
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--fungible-token-wasm",
                        type=str,
                        required=True,
                        help="Path to the compiled Fungible Token contract")
