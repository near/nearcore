import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from configured_logger import new_logger
from locust import constant_throughput, task
from common.base import NearUser
from common.inscription import MintInscription

logger = new_logger(level=logging.WARN)


class MintInscriptionUser(NearUser):

    @task
    def mint(self):
        self.send_tx(MintInscription(self.contract_account_id,
                                     self.account,
                                     "inscription-name-here",
                                     amt=100),
                     locust_name="Mint Inscription")

    def on_start(self):
        super().on_start()
        self.contract_account_id = self.environment.inscription_account_id
