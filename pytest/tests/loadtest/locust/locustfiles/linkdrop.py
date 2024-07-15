"""
A workload with Linkdrop operations.
"""

import logging
import pathlib
import random
import sys
import ed25519
import base58
import string

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key

from configured_logger import new_logger
from locust import constant_throughput, task
#from common.base import NearUser
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
        private_key, public_key = ed25519.create_keypair()
        # Convert keys to Base58 and add prefix
        sk = 'ed25519:' + base58.b58encode(
            private_key.to_bytes()).decode('ascii')
        pk = 'ed25519:' + base58.b58encode(
            public_key.to_bytes()).decode('ascii')
        public_keys = []
        public_keys.append(pk)
        tx = AddKey(self.linkdrop.account, self.account, public_keys,
                    self.drop_id)
        self.send_tx(tx, locust_name="Key added to the drop.")
        # Generate near name for the new account to be created via the claim.
        near_name = f"{random_string()}.near"
        node = NearNodeProxy(self.environment)
        tx_2 = ClaimDrop(self.linkdrop.account, near_name, pk, sk, node.node)
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


# Event listener for initializing Locust.
from locust import events


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    INIT_DONE.wait()
    node = NearNodeProxy(environment)
    linkdrop_contract_code = environment.parsed_options.linkdrop_wasm
    num_linkdrop_contracts = environment.parsed_options.num_linkdrop_contracts
    funding_account = NearUser.funding_account
    parent_id = funding_account.key.account_id
    funding_account.refresh_nonce(node.node)

    environment.linkdrop_contracts = []
    # TODO: Create accounts in parallel
    for i in range(num_linkdrop_contracts):
        account_id = environment.account_generator.random_account_id(
            parent_id, '_linkdrop')
        contract_key = key.Key.from_random(account_id)
        linkdrop_account = Account(contract_key)
        linkdrop_contract = LinkdropContract(linkdrop_account, linkdrop_account,
                                             linkdrop_contract_code)
        linkdrop_contract.install(node, funding_account)
        environment.linkdrop_contracts.append(linkdrop_contract)


# FT specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--linkdrop-wasm",
                        default="res/keypom.wasm",
                        help="Path to the compiled LinkDrop (Keypom) contract")
    parser.add_argument(
        "--num-linkdrop-contracts",
        type=int,
        required=False,
        default=5,
        help=
        "How many different Linkdrop contracts to spawn from this worker (Linkdrop contracts are never shared between workers)"
    )
