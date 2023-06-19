"""
A workload that simulates SocialDB traffic.
"""

import logging
import pathlib
import random
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

from configured_logger import new_logger
from locust import between, task
from common.base import NearUser
from common.social import Follow, InitSocialDbAccount, SubmitPost

logger = new_logger(level=logging.WARN)


class SocialDbUser(NearUser):
    """
    Registers itself on near.social in the setup phase, then starts posting,
    following, and liking posts.
    """
    wait_time = between(1, 3)  # random pause between transactions
    registered_users = []

    @task
    def follow(self):
        users_to_follow = [random.choice(SocialDbUser.registered_users)]
        self.send_tx(Follow(self.contract_account_id, self.account,
                            users_to_follow),
                     locust_name="Social Follow")

    @task
    def post(self):
        seed = random.randrange(2**32)
        len = random.randrange(100, 1000)
        post = self.generate_post(len, seed)
        self.send_tx(SubmitPost(self.contract_account_id, self.account, post),
                     locust_name="Social Post")

    def on_start(self):
        super().on_start()
        self.contract_account_id = self.environment.social_account_id

        self.send_tx(InitSocialDbAccount(self.contract_account_id,
                                         self.account),
                     locust_name="Init Social Account")
        logger.debug(
            f"user {self.account_id} ready to use SocialDB on {self.contract_account_id}"
        )

        SocialDbUser.registered_users.append(self.account_id)

    def generate_post(self, length: int, seed: int) -> str:
        sample_quotes = [
            "Despite the constant negative press covfefe",
            "Sorry losers and haters, but my I.Q. is one of the highest - and you all know it! Please don't feel so stupid or insecure, it's not your fault",
            "Windmills are the greatest threat in the US to both bald and golden eagles. Media claims fictional 'global warming' is worse.",
        ]
        quote = sample_quotes[seed % len(sample_quotes)]
        post = f"I, {self.account.key.account_id}, cannot resists to declare with pride: \n_{quote}_"
        while length > len(post):
            post = f"{post}\nI'll say it again: \n**{quote}**"

        return post[:length]
