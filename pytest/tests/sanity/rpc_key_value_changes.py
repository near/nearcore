# Spins up four nodes, deploy a smart contract to one node,
# and call various scenarios to trigger store changes.
# Check that the key changes are observable via `changes` RPC call.


import sys
import base58, base64
import json
import threading

import deepdiff

sys.path.append('lib')
from cluster import start_cluster, Key
from utils import load_binary_file
import transaction

nodes = start_cluster(4, 0, 1, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

# Plan:
# 1. Create a new account.
# 2. Observe the changes in the block where the receipt lands.

status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
new_account_id = 'rpc_key_value_changes'
create_account_tx = transaction.sign_create_account_tx(
    nodes[0].signer_key,
    new_account_id,
    5,
    base58.b58decode(latest_block_hash.encode('utf8'))
)
new_account_response = nodes[0].send_tx_and_wait(create_account_tx, 10)

state_changes_request = {
    "block_id": new_account_response['result']['receipts_outcome'][0]['block_hash'],
    "changes_type": "account_changes",
    "account_id": new_account_id,
}
expected_changes_response = {
    "block_hash": state_changes_request["block_id"],
    "changes": [
        {
            "cause": {
                "type": "receipt_processing",
                "receipt_hash": new_account_response['result']['receipts_outcome'][0]['id']
            },
            "type": "account_update",
            "change": {
                "account_id": new_account_id,
                "amount": "0",
                "locked": "0",
                "code_hash": "11111111111111111111111111111111",
                "storage_usage": 100,
            }
        }
    ]
}

for node in nodes:
    changes_response = nodes[0].get_changes(state_changes_request)['result']
    del changes_response['changes'][0]['change']['storage_paid_at']
    assert not deepdiff.DeepDiff(changes_response, expected_changes_response), \
        "query same changes gives different results (expected VS actual):\n%r\n%r" \
        % (expected_changes_response, changes_response)

# Plan:
# 1. Create a new account with an access key.
# 2. Observe the changes in the block where the receipt lands.
# 3. Remove the access key.
# 4. Observe the changes in the block where the receipt lands.

# re-use the key as a new account access key
new_key = Key(
    account_id='rpc_key_value_changes_full_access',
    pk=nodes[1].signer_key.pk,
    sk=nodes[1].signer_key.sk,
)

status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
create_account_tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
    creator_key=nodes[0].signer_key,
    new_account_id=new_key.account_id,
    new_key=new_key,
    balance=10**17,
    nonce=7,
    block_hash=base58.b58decode(latest_block_hash.encode('utf8'))
)
new_account_response = nodes[0].send_tx_and_wait(create_account_tx, 10)

state_changes_request = {
    "block_id": new_account_response['result']['receipts_outcome'][0]['block_hash'],
    "changes_type": "all_access_key_changes",
    "account_id": new_key.account_id,
}
expected_changes_response = {
    "block_hash": state_changes_request["block_id"],
    "changes": [
        {
            "cause": {
                "type": "receipt_processing",
                "receipt_hash": new_account_response["result"]["receipts_outcome"][0]["id"],
            },
            "type": "access_key_update",
            "change": {
                "public_key": new_key.pk,
                "access_key": {"nonce": 0, "permission": "FullAccess"},
            }
        }
    ]
}

for node in nodes:
    changes_response = nodes[0].get_changes(state_changes_request)['result']
    assert not deepdiff.DeepDiff(changes_response, expected_changes_response), \
        "query same changes gives different results (expected VS actual):\n%r\n%r" \
        % (expected_changes_response, changes_response)

status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
delete_access_key_tx = transaction.sign_delete_access_key_tx(
    signer_key=new_key,
    target_account_id=new_key.account_id,
    key_for_deletion=new_key,
    nonce=8,
    block_hash=base58.b58decode(latest_block_hash.encode('utf8'))
)
delete_access_key_response = nodes[1].send_tx_and_wait(delete_access_key_tx, 10)

state_changes_request = {
    "block_id": delete_access_key_response['result']['receipts_outcome'][0]['block_hash'],
    "changes_type": "all_access_key_changes",
    "account_id": new_key.account_id,
}
expected_changes_response = {
    "block_hash": state_changes_request["block_id"],
    "changes": [
        {
            "cause": {
                'type': 'transaction_processing',
                'tx_hash': delete_access_key_response['result']['transaction']['hash'],
            },
            "type": "access_key_update",
            "change": {
                "public_key": new_key.pk,
                "access_key": {"nonce": 8, "permission": "FullAccess"},
            }
        },
        {
            "cause": {
                "type": "receipt_processing",
                "receipt_hash": delete_access_key_response["result"]["receipts_outcome"][0]["id"]
            },
            "type": "access_key_deletion",
            "change": {
                "public_key": new_key.pk,
            }
        }
    ]
}

for node in nodes:
    changes_response = nodes[0].get_changes(state_changes_request)['result']
    assert not deepdiff.DeepDiff(changes_response, expected_changes_response), \
        "query same changes gives different results (expected VS actual):\n%r\n%r" \
        % (expected_changes_response, changes_response)


# Plan:
# 1. Deploy a contract.
# 2. Send two transactions to be included into the same block setting and overriding the value of
#    the same key (`my_key`).
# 3. Observe the changes in the block where the transaction outcome "lands".

status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
deploy_contract_tx = transaction.sign_deploy_contract_tx(
    nodes[0].signer_key,
    load_binary_file('../tests/hello.wasm'),
    10,
    base58.b58decode(latest_block_hash.encode('utf8'))
)
nodes[0].send_tx_and_wait(deploy_contract_tx, 10)

status = nodes[1].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']

def set_value_1():
    function_call_1_tx = transaction.sign_function_call_tx(
        nodes[0].signer_key,
        'setKeyValue',
        json.dumps({"key": "my_key", "value": "my_value_1"}).encode('utf-8'),
        100000000000,
        100000000000,
        20,
        base58.b58decode(latest_block_hash.encode('utf8'))
    )
    res = nodes[1].send_tx_and_wait(function_call_1_tx, 10)
function_call_1_thread = threading.Thread(target=set_value_1)
function_call_1_thread.start()

function_call_2_tx = transaction.sign_function_call_tx(
    nodes[0].signer_key,
    'setKeyValue',
    json.dumps({"key": "my_key", "value": "my_value_2"}).encode('utf-8'),
    100000000000,
    100000000000,
    30,
    base58.b58decode(latest_block_hash.encode('utf8'))
)
function_call_2_response = nodes[1].send_tx_and_wait(function_call_2_tx, 10)
assert function_call_2_response['result']['receipts_outcome'][0]['outcome']['status'] == {'SuccessValue': ''}, \
    "Expected successful execution, but the output was: %s" % function_call_2_response
function_call_1_thread.join()

tx_block_hash = function_call_2_response['result']['transaction_outcome']['block_hash']
tx_account_id = nodes[0].signer_key.account_id

state_changes_request = {
    "block_id": tx_block_hash,
    "changes_type": "data_changes",
    "account_id": tx_account_id,
    "key_prefix_base64": base64.b64encode(b"my_key").decode('utf-8'),
}

expected_changes_response = {
    "block_hash": state_changes_request["block_id"],
    "changes": [
        {
            'cause': {
                'type': 'receipt_processing',
            },
            'type': 'data_update',
            'change': {
                'key_base64': base64.b64encode(b"my_key").decode('utf-8'),
                'value_base64': base64.b64encode(b"my_value_1").decode('utf-8'),
            }
        },
        {
            'cause': {
                'type': 'receipt_processing',
                'receipt_hash': function_call_2_response['result']['receipts_outcome'][0]['id']
            },
            'type': 'data_update',
            'change': {
                'key_base64': base64.b64encode(b"my_key").decode('utf-8'),
                'value_base64': base64.b64encode(b"my_value_2").decode('utf-8'),
            }
        }
    ]
}


for node in nodes:
    changes_response = node.get_changes(state_changes_request)['result']
    # We fetch the transaction in a separate thread, so receiving the right receipt hash is a bit
    # involved process, nevermind.
    del changes_response['changes'][0]['cause']['receipt_hash']
    assert not deepdiff.DeepDiff(changes_response, expected_changes_response), \
        "query same changes gives different results (expected VS actual):\n%r\n%r" \
        % (expected_changes_response, changes_response)
