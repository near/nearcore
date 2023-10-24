from abc import abstractmethod
import json
import sys
import pathlib
import typing
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import transaction

from account import TGAS, NEAR_BASE
import key
from common.base import Account, Deploy, NearNodeProxy, Transaction, FunctionCall, INIT_DONE
from locust import events, runners
from transaction import create_function_call_action


class SocialDbSet(FunctionCall):

    def __init__(self, contract_id: str, sender: Account):
        super().__init__(sender, contract_id, "set")
        self.contract_id = contract_id
        self.sender = sender

    def sender_account(self) -> Account:
        return self.sender


class SubmitPost(SocialDbSet):

    def __init__(self, contract_id: str, sender: Account, content: str):
        super().__init__(contract_id, sender)
        self.content = content

    def args(self) -> dict:
        return social_post_args(self.sender.key.account_id, self.content)


class Follow(SocialDbSet):

    def __init__(self, contract_id: str, sender: Account,
                 follow_list: typing.List[str]):
        super().__init__(contract_id, sender)
        self.follow_list = follow_list

    def args(self) -> dict:
        follow_list = self.follow_list
        sender = self.sender.key.account_id
        return social_follow_args(sender, follow_list)


class InitSocialDB(Transaction):

    def __init__(self, contract: Account):
        super().__init__()
        self.contract = contract

    def sign_and_serialize(self, block_hash) -> bytes:
        # Call the #[init] function, no arguments
        call_new_action = create_function_call_action("new", "", 100 * TGAS, 0)

        # Set the status to "Live" to enable normal usage
        args = json.dumps({"status": "Live"}).encode('utf-8')
        call_set_status_action = create_function_call_action(
            "set_status", args, 100 * TGAS, 0)

        # Batch the two actions above into one transaction
        nonce = self.contract.use_nonce()
        key = self.contract.key
        return transaction.sign_and_serialize_transaction(
            key.account_id, nonce, [call_new_action, call_set_status_action],
            block_hash, key.account_id, key.decoded_pk(), key.decoded_sk())

    def sender_account(self) -> Account:
        return self.contract


class InitSocialDbAccount(FunctionCall):
    """
    Send initial storage balance to ensure the account can use social DB.

    Technically, we could also rely on lazy initialization and just send enough
    balance with each request. But a typical user sends balance ahead of time.
    """

    def __init__(self, contract_id: str, account: Account):
        super().__init__(account,
                         contract_id,
                         "storage_deposit",
                         balance=1 * NEAR_BASE)
        self.contract_id = contract_id
        self.account = account

    def args(self) -> dict:
        return {"account_id": self.account.key.account_id}

    def sender_account(self) -> Account:
        return self.account


def social_db_build_index_obj(key_list_pairs: dict) -> dict:
    """
    JSON serializes the key - value-list pairs to be included in a SocialDB set message.

    To elaborate a bit more, SocialDB expects for example
    ```json
    "index": { "graph": value_string } 
    ```
    where value_string = 
    ```json
    "[{\"key\":\"follow\",\"value\":{\"type\":\"follow\",\"accountId\":\"pagodaplatform.near\"}}]"
    ```
    So it's really JSON nested inside a JSON string.
    And worse, the nested JSON is always a list of objects with "key" and "value" fields.
    This method unfolds this format from a leaner definition, using a list of pairs to
    define each `value_string`.
    A dict instead of a list of tuples doesn't work because keys can be duplicated.
    """

    def serialize_values(values: typing.List[typing.Tuple[str, dict]]):
        return json.dumps([{"key": k, "value": v} for k, v in values])

    return {
        key: serialize_values(values) for key, values in key_list_pairs.items()
    }


def social_db_set_msg(sender: str, values: dict, index: dict) -> dict:
    """
    Construct a SocialDB `set` function argument.

    The output will be of the form:
    ```json
    {
      "data": {
        "key1": value1,
        "key3": value2,
        "key4": value3,
        "index": {
          "index_key1": "[{\"index_key_1_key_A\":\"index_key_1_value_A\"}]",
          "index_key2": "[{\"index_key_2_key_A\":\"index_key_2_value_A\"},{\"index_key_2_key_B\":\"index_key_2_value_B\"}]",
        }
      }
    }
    ```
    """
    updates = values.copy()
    updates["index"] = social_db_build_index_obj(index)
    msg = {"data": {sender: updates}}
    return msg


def social_follow_args(sender: str, follow_list: typing.List[str]) -> dict:
    follow_map = {}
    graph = []
    notify = []
    for user in follow_list:
        follow_map[user] = ""
        graph.append(("follow", {"type": "follow", "accountId": user}))
        notify.append((user, {"type": "follow"}))

    values = {
        "graph": {
            "follow": follow_map
        },
    }
    index = {"graph": graph, "notify": notify}
    return social_db_set_msg(sender, values, index)


def social_post_args(sender: str, text: str) -> dict:
    values = {"post": {"main": json.dumps({"type": "md", "text": text})}}
    index = {"post": [("main", {"type": "md"})]}
    msg = social_db_set_msg(sender, values, index)
    return msg


class TestSocialDbSetMsg(unittest.TestCase):

    def test_follow(self):
        sender = "alice.near"
        follow_list = ["bob.near"]
        parsed_msg = social_follow_args(sender, follow_list)
        expected_msg = {
            "data": {
                "alice.near": {
                    "graph": {
                        "follow": {
                            "bob.near": ""
                        }
                    },
                    "index": {
                        "graph":
                            "[{\"key\": \"follow\", \"value\": {\"type\": \"follow\", \"accountId\": \"bob.near\"}}]",
                        "notify":
                            "[{\"key\": \"bob.near\", \"value\": {\"type\": \"follow\"}}]"
                    }
                }
            }
        }
        self.maxDiff = 2000  # print large diffs
        self.assertEqual(parsed_msg, expected_msg)

    def test_mass_follow(self):
        sender = "alice.near"
        follow_list = ["bob.near", "caroline.near", "david.near"]
        parsed_msg = social_follow_args(sender, follow_list)
        expected_msg = {
            "data": {
                "alice.near": {
                    "graph": {
                        "follow": {
                            "bob.near": "",
                            "caroline.near": "",
                            "david.near": "",
                        }
                    },
                    "index": {
                        "graph":
                            "[{\"key\": \"follow\", \"value\": {\"type\": \"follow\", \"accountId\": \"bob.near\"}},"
                            " {\"key\": \"follow\", \"value\": {\"type\": \"follow\", \"accountId\": \"caroline.near\"}},"
                            " {\"key\": \"follow\", \"value\": {\"type\": \"follow\", \"accountId\": \"david.near\"}}]",
                        "notify":
                            "[{\"key\": \"bob.near\", \"value\": {\"type\": \"follow\"}},"
                            " {\"key\": \"caroline.near\", \"value\": {\"type\": \"follow\"}},"
                            " {\"key\": \"david.near\", \"value\": {\"type\": \"follow\"}}]"
                    }
                }
            }
        }
        self.maxDiff = 2000  # print large diffs
        self.assertEqual(parsed_msg, expected_msg)

    def test_post(self):
        sender = "alice.near"
        text = "#Title\n\nbody"
        parsed_msg = social_post_args(sender, text)
        expected_msg = {
            "data": {
                "alice.near": {
                    "post": {
                        "main":
                            "{\"type\": \"md\", \"text\": \"#Title\\n\\nbody\"}"
                    },
                    "index": {
                        "post":
                            "[{\"key\": \"main\", \"value\": {\"type\": \"md\"}}]"
                    }
                }
            }
        }
        self.maxDiff = 2000  # print large diffs
        self.assertEqual(parsed_msg, expected_msg)


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    INIT_DONE.wait()
    # `master_funding_account` is the same on all runners, allowing to share a
    # single instance of SocialDB in its `social` sub account
    funding_account = environment.master_funding_account
    environment.social_account_id = f"social{environment.parsed_options.run_id}.{funding_account.key.account_id}"

    # Create SocialDB account, unless we are a worker, in which case the master already did it
    if not isinstance(environment.runner, runners.WorkerRunner):
        social_contract_code = environment.parsed_options.social_db_wasm
        contract_key = key.Key.from_seed_testonly(environment.social_account_id)
        social_account = Account(contract_key)

        node = NearNodeProxy(environment)
        existed = node.prepare_account(social_account, funding_account, 50000,
                                       "create contract account")
        if not existed:
            node.send_tx_retry(
                Deploy(social_account, social_contract_code, "Social DB"),
                "deploy socialDB contract")
            node.send_tx_retry(InitSocialDB(social_account),
                               "init socialDB contract")


# Social specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--social-db-wasm",
        default="res/social_db.wasm",
        help=
        "Path to the compiled SocialDB contract, get it from https://github.com/NearSocial/social-db/tree/aa7fafaac92a7dd267993d6c210246420a561370/res"
    )
