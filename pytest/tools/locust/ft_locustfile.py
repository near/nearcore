"""
TODO
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

