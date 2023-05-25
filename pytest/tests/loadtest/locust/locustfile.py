"""
TODO
"""

import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))

from configured_logger import new_logger
from locust import between, task, events
from common.base import Account, CreateSubAccount, Deploy, NearUser, send_transaction
from common.ft import FTContract, InitFTAccount, TransferFT

logger = new_logger(level=logging.WARN)


class FTTransferUser(NearUser):
    wait_time = between(1, 3)  # random pause between transactions

    @task
    def ft_transfer(self):
        receiver = self.ft.random_receiver(self.account_id)
        tx = TransferFT(self.ft.account, self.account, receiver, how_much=1)
        self.send_tx(tx, locust_name="FT transfer")

    def on_start(self):
        super().on_start()

        self.ft = random.choice(self.environment.ft_contracts)
        self.ft.register_user(self)
        logger.debug(
            f"{self.account_id} ready to use FT contract {self.ft.account.key.account_id}"
        )
