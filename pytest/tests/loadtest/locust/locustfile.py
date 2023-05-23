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
from common.ft import InitFT, InitFTAccount, TransferFT
from common.social import Follow, InitSocialDB, InitSocialDbAccount

logger = new_logger(level=logging.WARN)


class FTTransferUser(NearUser):
    """
    Registers itself on an FT contract in the setup phase, then just sends FTs to
    random users.
    """
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


class SocialDbUser(NearUser):
    """
    Registers itself on near.social in the setup phase, then starts posting,
    following, and liking posts.
    """
    wait_time = between(1, 3)  # random pause between transactions
    registered_users = []

    @task
    def follow(self):
        rng = random.Random()
        users_to_follow = [rng.choice(SocialDbUser.registered_users)]
        self.send_tx(
            Follow(self.contract_account_id, self.account, users_to_follow))

    # @task
    # def post(self):
    #     #TODO:
    #     post = lorem_ipsum()
    #     send(tryx(post))

    # @task
    # def like(self):
    #     #TODO:
    #     post_id = posts_pool.random_with_pareto_ditro()
    #     send(tx(post))

    def on_start(self):
        super().on_start()
        self.contract_account_id = self.environment.social_account_id

        self.send_tx(InitSocialDbAccount(self.contract_account, self.account))
        logger.debug(
            f"user {self.account_id} ready to use SocialDB on {self.contract_account.key.account_id}"
        )

        SocialDbUser.registered_users.append(self.account_id)
