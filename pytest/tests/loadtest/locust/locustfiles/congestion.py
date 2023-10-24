"""
A workload that generates congestion.
"""

import logging
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from configured_logger import new_logger
from locust import between, task
from common.base import NearUser
from common.congestion import ComputeSha256, ComputeSum

logger = new_logger(level=logging.WARN)


class CongestionUser(NearUser):
    """
    Runs a resource-heavy workload that is likely to cause congestion.
    """
    wait_time = between(1, 3)  # random pause between transactions

    @task
    def compute_sha256(self):
        self.send_tx(ComputeSha256(self.contract_account_id, self.account,
                                   100000),
                     locust_name="SHA256, 100 KiB")

    @task
    def compute_sum(self):
        self.send_tx(ComputeSum(self.contract_account_id, self.account, 250),
                     locust_name="Sum, 250 TGas")

    def on_start(self):
        super().on_start()
        self.contract_account_id = self.environment.congestion_account_id
