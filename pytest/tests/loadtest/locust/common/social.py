import json
import sys
import pathlib
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[4] / 'lib'))
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import transaction

from account import TGAS, NEAR_BASE
import cluster
import key
from common.base import Account, CreateSubAccount, Deploy, NearUser, Transaction, send_transaction
from locust import events, runners
from transaction import create_function_call_action


# class SubmitPost(Transaction):

#     def __init__(self, social, sender, post_content, tgas=300):
#         super().__init__()
#         self.social = social
#         self.sender = sender
#         self.post_content = post_content
#         self.attached_tgas = tgas

#     def sign_and_serialize(self, block_hash):
#         # TODO
#         return tx


# class LikePost(Transaction):

#     def __init__(self, social, sender, post_id, tgas=300):
#         super().__init__()
#         self.social = social
#         self.sender = sender
#         self.post_id = post_id
#         self.attached_tgas = tgas

#     def sign_and_serialize(self, block_hash):
#         # TODO
#         return tx


class Follow(Transaction):

    def __init__(self, contract_id: str, sender: Account, follow_list: "list[str]"):
        super().__init__()
        self.contract_id = contract_id
        self.sender = sender
        self.follow_list = follow_list

    def sign_and_serialize(self, block_hash):
        follow_map = {}
        graph_map = { "follow": {} }
        notify_map = {}
        for user in self.follow_list:
            follow_map[user] = ""
            graph_map["follow"][user] = { "type": "follow", "accountId": user }
            notify_map[user] = { "type": "follow" }
        
        values = {
            "graph": follow_map,
        }
        index = {
            "graph": graph_map,
            "notify": notify_map
        }
        args = social_db_set_msg(self.sender.key.account_id, values, index)
        return transaction.sign_function_call_tx(
            self.sender.key,
            self.contract_id,
            "set",
            args.encode('utf-8'),
            300 * TGAS,
            0,
            self.sender.use_nonce(),
            block_hash)


class InitSocialDB(Transaction):

    def __init__(self, contract: Account):
        super().__init__()
        self.contract = contract

    def sign_and_serialize(self, block_hash):
        # Call the #[init] function, no arguments
        call_new_action = create_function_call_action("new", "", 100 * TGAS, 0)
        
        # Set the status to "Live" to enable normal usage
        args = json.dumps({"status": "Live"}).encode('utf-8')
        call_set_status_action = create_function_call_action("set_status", args, 100 * TGAS, 0)
        
        # Batch the two actions above into one transaction
        nonce = self.contract.use_nonce()
        key = self.contract.key
        return transaction.sign_and_serialize_transaction(
            key.account_id,
            nonce,
            [call_new_action, call_set_status_action],
            block_hash,
            key.account_id,
            key.decoded_pk(),
            key.decoded_sk()
        )


class InitSocialDbAccount(Transaction):
    """
    Send initial storage balance to ensure the account can use social DB.
    
    Technically, we could also rely on lazy initialization and just send enough
    balance with each request. But a typical user sends balance ahead of time.
    """

    def __init__(self, contract_id: str, account: Account):
        super().__init__()
        self.contract_id = contract_id
        self.account = account

    def sign_and_serialize(self, block_hash):
        args = json.dumps({"account_id": self.account.key.account_id})
        return transaction.sign_function_call_tx(self.account.key,
                                               self.contract_id,
                                               "storage_deposit",
                                               args.encode('utf-8'),
                                               300 * TGAS,
                                               1 * NEAR_BASE,
                                               self.account.use_nonce(),
                                               block_hash)


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
    This method unfolds this format from a leaner definition, using a dict to
    define each `value_string`.
    """
    obj = {}
    for index_key,values in key_list_pairs.items():
        unfolded_list = [{"key": k, "value": v} for k,v in values.items()]
        obj[index_key] = json.dumps(unfolded_list)
    return obj


def social_db_set_msg(sender: str, values: dict, index: dict) -> str:
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
    updates = values
    updates["index"] = social_db_build_index_obj(index)
    msg = {
        "data": {
            sender: updates
        }
    }
    return json.dumps(msg)


class TestSocialDbSetMsg(unittest.TestCase):
    def assertJSONEqual(self, actual_json, expected_json):
        expected_obj = json.loads(expected_json)
        actual_obj = json.loads(actual_json)
        self.assertEqual(expected_obj, actual_obj)

    def runTest(self):
        sender = "alice.near"
        values = {"graph": { "follow": { "bob.near": "" } }}
        index = {
            "graph": {"follow": { "type": "follow", "accountId": "bob.near" }},
            "notify": {"bob.near": { "type": "follow" }}
        }
        msg = social_db_set_msg(sender, values, index)
        parsed_msg = json.loads(msg)
        expected_msg = {
            "data": {
                "alice.near": {
                    "graph": {
                        "follow": {
                            "bob.near": ""
                        }
                    },
                    "index": {
                        "graph": "[{\"key\": \"follow\", \"value\": {\"type\": \"follow\", \"accountId\": \"bob.near\"}}]",
                        "notify": "[{\"key\": \"bob.near\", \"value\": {\"type\": \"follow\"}}]"
                    }
                }
            }
        }
        self.maxDiff = 2000
        self.assertEqual(parsed_msg, expected_msg)


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    # `master_funding_account` is the same on all runners, allowing to share a
    # single instance of SocialDB in its `social` sub account
    funding_account = environment.master_funding_account
    environment.social_account_id = f"social.{funding_account.key.account_id}"
    
    # Create SocialDB account, unless we are a worker, in which case the master already did it
    if not isinstance(environment.runner, runners.WorkerRunner):
        social_contract_code = environment.parsed_options.social_db_wasm
        contract_key = key.Key.from_random(environment.social_account_id)
        social_account = Account(contract_key)
        
        # Note: These setup requests are not tracked by locust because we use our own http session
        host, port = environment.host.split(":")
        node = cluster.RpcNode(host, port)
        
        send_transaction(
        node, CreateSubAccount(funding_account, social_account.key,
                                balance=50000.0))
        social_account.refresh_nonce(node)
        send_transaction(node, Deploy(social_account, social_contract_code, "Social DB"))
        send_transaction(node, InitSocialDB(social_account))


# Social specific CLI args
@events.init_command_line_parser.add_listener
def _(parser):
 parser.add_argument("--social-db-wasm",
                        type=str,
                        required=True,
                        help="Path to the compiled SocialDB contract, get it from https://github.com/NearSocial/social-db/tree/aa7fafaac92a7dd267993d6c210246420a561370/res")
