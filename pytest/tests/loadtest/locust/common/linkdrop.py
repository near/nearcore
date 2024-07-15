import random
import string
import sys
import pathlib
import typing
from locust import events
import time
import json
import ed25519
import base58

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))

import key
from common.base import Account, Deploy, NearNodeProxy, NearUser, FunctionCall, INIT_DONE, Transaction
import transaction
from account import TGAS


class LinkdropContract:
    INIT_BALANCE = NearUser.INIT_BALANCE

    def __init__(self, account: Account, linkdrop_distributor: Account,
                 code: str):
        self.account = account
        self.linkdrop_distributor = linkdrop_distributor
        self.code = code

    def install(self, node: NearNodeProxy, parent: Account):
        """
        Deploy and initialize the contract on chain.
        The account is created if it doesn't exist yet.
        """
        existed = node.prepare_account(self.account, parent,
                                       LinkdropContract.INIT_BALANCE,
                                       "create contract account")
        if not existed:
            node.send_tx_retry(Deploy(self.account, self.code, "LINKDROP"),
                               "Deploy Linkdrop")
            self.init_contract(node)

    def init_contract(self, node: NearNodeProxy):
        node.send_tx_retry(InitDropContract(self.account), "Init Drop Contract")

    def create_drop(self, user: NearUser) -> str:
        drop_id = str(random.randint(1_000_000_000, 10**38 - 1))
        #Using send_tx_retry gave a lot of errors with drop_id already exists although we are using a random number
        user.send_tx(InitDrop(self.account, user.account, drop_id),
                     locust_name="Create Drop Config")
        return drop_id


class InitDropContract(FunctionCall):

    def __init__(self, contract: Account):
        super().__init__(contract, contract.key.account_id, "new")
        self.contract = contract

    def args(self) -> dict:
        return {
            "owner_id": self.contract.key.account_id,
            "root_account": "near",
            "contract_metadata": {
                "version": "v2",
                "link": "google.com"
            }
        }

    def sender_account(self) -> Account:
        return self.contract


class InitDrop(FunctionCall):

    def __init__(self, contract: Account, account: Account, drop_id: str):
        super().__init__(account,
                         contract.key.account_id,
                         "create_drop",
                         balance=int(1E23))
        self.contract = contract
        self.account = account
        self.drop_id = drop_id

    def args(self) -> dict:
        return {
            "public_keys": [],
            "deposit_per_use": "100000000000000000000000",
            "drop_id": self.drop_id
        }

    def sender_account(self) -> Account:
        return self.account


class AddKey(FunctionCall):

    def __init__(self, linkdrop: Account, sender: Account, public_keys,
                 drop_id: str):
        super().__init__(sender,
                         linkdrop.key.account_id,
                         "add_keys",
                         balance=int(15E22))
        self.linkdrop = linkdrop
        self.sender = sender
        self.public_keys = public_keys
        self.drop_id = drop_id

    def args(self) -> dict:
        return {"public_keys": self.public_keys, "drop_id": self.drop_id}

    def sender_account(self) -> Account:
        return self.sender


class ClaimDrop(Transaction):

    def __init__(
        self,
        sender: Account,
        new_account_id: str,
        la_public_key,  #this is the limited access key added to the linkdrop contract already
        la_secret_key,  #same as above
        node,
        balance: int = 0,
    ):
        super().__init__()
        self.sender = sender
        self.receiver_id = sender.key.account_id
        self.method = "create_account_and_claim"
        self.new_account_id = new_account_id
        # defensive cast to avoid serialization bugs when float balance is
        # provided despite type hint
        self.balance = int(balance)
        self.node = node

        #Create a new key pair, which would be a full access key to your account
        private_key, public_key = ed25519.create_keypair()
        # Convert keys to Base58 and add prefix
        sk = 'ed25519:' + base58.b58encode(
            private_key.to_bytes()).decode('ascii')
        pk = 'ed25519:' + base58.b58encode(
            public_key.to_bytes()).decode('ascii')
        #need it for functions args
        self.pk = pk

        #Creating a signer with the limited access key
        self.sender.key.sk = la_secret_key
        self.sender.key.pk = la_public_key
        self.la_public_key = la_public_key

    def args(self) -> dict:
        return {
            "new_account_id": self.new_account_id,
            "new_public_key": self.pk
        }

    def sign(self, block_hash) -> transaction.SignedTransaction:
        return transaction.sign_function_call_transaction(
            self.sender.key, self.receiver_id, self.method,
            json.dumps(self.args()).encode('utf-8'), 100 * TGAS, self.balance,
            self.sender.get_nonce_for_pk(self.node, self.sender.key.account_id,
                                         self.la_public_key) + 1, block_hash)

    def sender_account(self) -> Account:
        return self.sender
