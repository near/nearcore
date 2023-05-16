"""
TODO
"""


import sys
import pathlib
import logging

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

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
    wait_time = between(1, 3) # random pause between transactions

    @task
    def ft_transfer(self):
        logger.debug(f"START FT TRANSFER {self.id}")
        receiver = NearUser.random_account_id(but_not=self.id)
        self.send_tx(
            TransferFT(
                self.contract_account,
                self.account,
                receiver,
                how_much=1
                )
            )
        logger.debug(f"FT TRANSFER {self.id} DONE")

    def on_start(self):
        super().on_start()

        self.contract_account = FT_ACCOUNT

        logger.debug(f"starting user {self.id} init")
        self.send_tx(InitFTAccount(self.contract_account, self.account))
        logger.debug(f"user {self.account_id} InitFTAccount done")
        self.send_tx(TransferFT(self.contract_account, self.contract_account, self.account_id, how_much=1E8))
        logger.debug(f"user {self.id} TransferFT done, user ready")

# called once per process before user initialization
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    funding_account = NearUser.funding_account
    contract_code = environment.parsed_options.fungible_token_wasm

    # TODO; more than one contract
    contract_key = key.Key.from_random(f"ft.{funding_account.key.account_id}")
    ft_account = Account(contract_key)

    global FT_ACCOUNT
    FT_ACCOUNT = ft_account

    # Note: These setup requests are not tracked by locust because we use our own http session
    host, port = environment.host.split(":")
    node = cluster.RpcNode(host, port)
    send_transaction(node, CreateSubAccount(funding_account, ft_account.key, balance=50000.0))
    ft_account.refresh_nonce(node)
    logger.info("INIT Deploying")
    send_transaction(node, Deploy(ft_account, contract_code, "FT"))
    logger.info("INIT FT init")
    send_transaction(node, InitFT(ft_account))

# Add custom CLI args here, will be available in `environment.parsed_options`
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--fungible-token-wasm", type=str, required=True, help="Path to the compiled Fungible Token contract")

