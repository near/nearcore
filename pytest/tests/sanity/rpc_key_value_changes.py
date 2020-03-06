# Spins up four nodes, deploy an smart contract to one node,
# and call a set key method on it.
# Check that the key changes are observable via `changes` RPC call.


import sys
import base58, base64
import json
import threading

import deepdiff

sys.path.append('lib')
from cluster import start_cluster
from transaction import sign_create_account_tx, sign_deploy_contract_tx, sign_function_call_tx
from utils import load_binary_file

nodes = start_cluster(4, 0, 1, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
new_account_id = 'rpc_key_value_changes'
create_account_tx = sign_create_account_tx(
    nodes[0].signer_key,
    new_account_id,
    9,
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
        "query same changes gives different results:\n%r" \
        % changes_response

status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
deploy_contract_tx = sign_deploy_contract_tx(
    nodes[0].signer_key,
    load_binary_file('../tests/hello.wasm'),
    10,
    base58.b58decode(latest_block_hash.encode('utf8'))
)
nodes[0].send_tx_and_wait(deploy_contract_tx, 10)

status = nodes[1].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']

def set_value_1():
    function_call_1_tx = sign_function_call_tx(
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

function_call_2_tx = sign_function_call_tx(
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

# send method=changes params:={"block_id": block_hash, "changes_type": "data_changes", "account_id": account_id, "key_prefix": key_prefix_as_base64_encoded_string}
# e.g. method=changes params:={"block_id": "8jT2x22z757m378fsKQe2JHin1k56k1jM1sQoqeqyABx", "changes_type": "data_changes", "account_id": "test0", "key_prefix": "bXlrZXk="}
# expect:
# {
#    "id": "dontcare",
#    "jsonrpc": "2.0",
#    "result": {
#        "block_hash": "8jT2x22z757m378fsKQe2JHin1k56k1jM1sQoqeqyABx",
#        "changes": [
#            {
#                "cause": {
#                    "receipt_hash": "7ZYAy37zjc6ectrUifocCXxgZTdXYTce1gAiXLE8vv9a",
#                    "type": "receipt_processing"
#                },
#                "change": {
#                    "key_base64": "bXlrZXk="  # it is b"mykey" in base64 encoding
#                    "value_base64": "bXl2YWx1ZQ=="  # it is b"myvalue" in base64 encoding
#                },
#                "type": "data_update"
#            }
#        ]
#    }
#}

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
        "query same changes gives different results:\n%r" \
        % (changes_response)
