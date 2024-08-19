"""
A workload with Linkdrop operations.
"""

import logging
import pathlib
import random
import sys
import base58
import string

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key
from configured_logger import new_logger
from locust import constant_throughput, task
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, INIT_DONE
from common.linkdrop import LinkdropContract, AddKey, ClaimDrop

logger = new_logger(level=logging.WARN)


class LinkdropUser(NearUser):
    """
    Funder creates a drop on Keypom contract. Add keys to the drop.
    Claimer claims the drop.
    """
    # Only simple claims right now. Do FT, NFT etc later.

    # Each Locust user will try to send one transaction per second.
    # See https://docs.locust.io/en/stable/api.html#locust.wait_time.constant_throughput.
    wait_time = constant_throughput(1.0)

    @task
    def create_and_claim_drop(self):
        # Generate a random key pair
        keypair = key.Key.implicit_account()
        tx = AddKey(self.linkdrop.account, self.account, keypair.pk,
                    self.drop_id)
        self.send_tx(tx, locust_name="Key added to the drop.")
        # Generate near name for the new account to be created via the claim.
        near_name = f"{random_string()}.near"
        node = NearNodeProxy(self.environment)
        tx_2 = ClaimDrop(self.linkdrop.account, near_name, keypair.pk,
                         keypair.sk, node.node)
        result2 = self.send_tx(tx_2, locust_name="Linkdrop Claimed")

    #Create a simple drop config on Linkdrop contract
    def on_start(self):
        super().on_start()
        self.linkdrop = random.choice(self.environment.linkdrop_contracts)
        # Keypom contract does not need registration and distibution funds like FT.
        # Just create a drop.
        self.drop_id = self.linkdrop.create_drop(self)
        logger.debug(
            f"{self.account_id} ready to use Linkdrop contract {self.linkdrop.account.key.account_id}"
        )


def random_string(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))
