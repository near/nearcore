"""
A workload with Fungible Token operations.
"""

import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from configured_logger import new_logger
from locust import constant_throughput, task
from common.base import NearUser
from common.ft import TransferFT

logger = new_logger(level=logging.WARN)


class FTTransferUser(NearUser):
    """
    Registers itself on an FT contract in the setup phase, then just sends FTs to
    random users.
    """
    # Each Locust user will try to send one transaction per second.
    # See https://docs.locust.io/en/stable/api.html#locust.wait_time.constant_throughput.
    wait_time = constant_throughput(1.0)

    @task
    def ft_transfer(self):
        receiver = self.ft.random_receiver(self.account_id)
        tx = TransferFT(self.ft.account, self.account, receiver, how_much=1)
        self.send_tx_async(tx, locust_name="FT transfer")

    def on_start(self):
        super().on_start()
        self.ft = random.choice(self.environment.ft_contracts)
        self.ft.register_user(self)
        logger.debug(
            f"{self.account_id} ready to use FT contract {self.ft.account.key.account_id}"
        )
