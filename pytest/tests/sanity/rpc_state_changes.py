# Spins up four nodes, deploy a smart contract to one node,
# and call various scenarios to trigger store changes.
# Check that the key changes are observable via `changes` RPC call.

import sys
import base58, base64
import json
import threading

import deepdiff

sys.path.append('lib')
from cluster import start_cluster
from key import Key
from utils import load_binary_file
import transaction

nodes = start_cluster(
    4, 0, 1, None,
    [["epoch_length", 1000], ["block_producer_kickout_threshold", 80]], {})


def assert_changes_in_block_response(request, expected_response):
    for node_index, node in enumerate(nodes):
        response = node.get_changes_in_block(request)
        assert 'result' in response, "the request did not succeed: %r" % response
        response = response['result']
        diff = deepdiff.DeepDiff(expected_response, response)
        assert not diff, \
            "query node #%d same changes gives different results %r (expected VS actual):\n%r\n%r" \
            % (node_index, diff, expected_response, response)


def assert_changes_response(request, expected_response, **kwargs):
    for node_index, node in enumerate(nodes):
        response = node.get_changes(request)
        assert 'result' in response, "the request did not succeed: %r" % response
        response = response['result']
        diff = deepdiff.DeepDiff(expected_response, response, **kwargs)
        assert not diff, \
            "query node #%d same changes gives different results %r (expected VS actual):\n%r\n%r" \
            % (node_index, diff, expected_response, response)


def test_changes_with_new_account_with_access_key():
    """
    Plan:
    1. Create a new account with an access key.
    2. Observe the changes in the block where the receipt lands.
    3. Remove the access key.
    4. Observe the changes in the block where the receipt lands.
    """

    # re-use the key as a new account access key
    new_key = Key(
        account_id='rpc_key_value_changes_full_access',
        pk=nodes[1].signer_key.pk,
        sk=nodes[1].signer_key.sk,
    )

    # Step 1
    status = nodes[0].get_status()
    latest_block_hash = status['sync_info']['latest_block_hash']
    create_account_tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
        creator_key=nodes[0].signer_key,
        new_account_id=new_key.account_id,
        new_key=new_key,
        balance=10**24,
        nonce=7,
        block_hash=base58.b58decode(latest_block_hash.encode('utf8')))
    new_account_response = nodes[0].send_tx_and_wait(create_account_tx, 10)

    # Step 2
    block_hash = new_account_response['result']['receipts_outcome'][0][
        'block_hash']
    assert_changes_in_block_response(request={"block_id": block_hash},
                                     expected_response={
                                         "block_hash":
                                             block_hash,
                                         "changes": [{
                                             "type": "account_touched",
                                             "account_id": new_key.account_id,
                                         }, {
                                             "type": "access_key_touched",
                                             "account_id": new_key.account_id,
                                         }]
                                     })

    base_request = {
        "block_id": block_hash,
        "changes_type": "all_access_key_changes",
    }
    for request in [
            # Test empty account_ids
        {
            **base_request, "account_ids": []
        },
            # Test an account_id that is a prefix of the original account_id.
        {
            **base_request, "account_ids": [new_key.account_id[:-1]]
        },
            # Test an account_id that has the original account_id as a prefix.
        {
            **base_request, "account_ids": [new_key.account_id + '_extra']
        },
    ]:
        assert_changes_response(request=request,
                                expected_response={
                                    "block_hash": block_hash,
                                    "changes": []
                                })

    # Test happy-path
    block_header = nodes[0].get_block(block_hash)['result']['header']
    prev_block_header = nodes[0].get_block(block_header['prev_hash'])['result']['header']
    nonce = prev_block_header['height'] * 1000000
    expected_response = {
        "block_hash":
            block_hash,
        "changes": [{
            "cause": {
                "type":
                    "receipt_processing",
                "receipt_hash":
                    new_account_response["result"]["receipts_outcome"][0]["id"],
            },
            "type": "access_key_update",
            "change": {
                "account_id": new_key.account_id,
                "public_key": new_key.pk,
                "access_key": {
                    "nonce": nonce,
                    "permission": "FullAccess"
                },
            }
        }]
    }
    for request in [
        {
            "block_id": block_hash,
            "changes_type": "all_access_key_changes",
            "account_ids": [new_key.account_id],
        },
        {
            "block_id":
                block_hash,
            "changes_type":
                "all_access_key_changes",
            "account_ids": [
                new_key.account_id + '_non_existing1', new_key.account_id,
                new_key.account_id + '_non_existing2'
            ],
        },
    ]:
        assert_changes_response(request=request,
                                expected_response=expected_response)

    # Step 3
    status = nodes[0].get_status()
    latest_block_hash = status['sync_info']['latest_block_hash']
    nonce += 8
    delete_access_key_tx = transaction.sign_delete_access_key_tx(
        signer_key=new_key,
        target_account_id=new_key.account_id,
        key_for_deletion=new_key,
        nonce=nonce,
        block_hash=base58.b58decode(latest_block_hash.encode('utf8')))
    delete_access_key_response = nodes[1].send_tx_and_wait(
        delete_access_key_tx, 10)

    # Step 4
    block_hash = delete_access_key_response['result']['receipts_outcome'][0][
        'block_hash']
    assert_changes_in_block_response(request={"block_id": block_hash},
                                     expected_response={
                                         "block_hash":
                                             block_hash,
                                         "changes": [{
                                             "type": "account_touched",
                                             "account_id": new_key.account_id,
                                         }, {
                                             "type": "access_key_touched",
                                             "account_id": new_key.account_id,
                                         }]
                                     })

    base_request = {
        "block_id": block_hash,
        "changes_type": "all_access_key_changes",
    }
    for request in [
            # Test empty account_ids
        {
            **base_request, "account_ids": []
        },
            # Test an account_id that is a prefix of the original account_id
        {
            **base_request, "account_ids": [new_key.account_id[:-1]]
        },
            # Test an account_id that has the original account_id as a prefix
        {
            **base_request, "account_ids": [new_key.account_id + '_extra']
        },
            # Test empty keys in single_access_key_changes request
        {
            "block_id": block_hash,
            "changes_type": "single_access_key_changes",
            "keys": []
        },
            # Test non-existing account_id
        {
            "block_id":
                block_hash,
            "changes_type":
                "single_access_key_changes",
            "keys": [{
                "account_id": new_key.account_id + '_non_existing1',
                "public_key": new_key.pk
            },],
        },
            # Test non-existing public_key for an existing account_id
        {
            "block_id":
                block_hash,
            "changes_type":
                "single_access_key_changes",
            "keys": [{
                "account_id": new_key.account_id,
                "public_key": new_key.pk[:-3] + 'aaa'
            },],
        },
    ]:
        assert_changes_response(request=request,
                                expected_response={
                                    "block_hash": block_hash,
                                    "changes": []
                                })

    # Test happy-path
    expected_response = {
        "block_hash":
            block_hash,
        "changes": [{
            "cause": {
                'type':
                    'transaction_processing',
                'tx_hash':
                    delete_access_key_response['result']['transaction']['hash'],
            },
            "type": "access_key_update",
            "change": {
                "account_id": new_key.account_id,
                "public_key": new_key.pk,
                "access_key": {
                    "nonce": nonce,
                    "permission": "FullAccess"
                },
            }
        }, {
            "cause": {
                "type":
                    "receipt_processing",
                "receipt_hash":
                    delete_access_key_response["result"]["receipts_outcome"][0]
                    ["id"]
            },
            "type": "access_key_deletion",
            "change": {
                "account_id": new_key.account_id,
                "public_key": new_key.pk,
            }
        }]
    }

    for request in [
        {
            "block_id": block_hash,
            "changes_type": "all_access_key_changes",
            "account_ids": [new_key.account_id],
        },
        {
            "block_id":
                block_hash,
            "changes_type":
                "all_access_key_changes",
            "account_ids": [
                new_key.account_id + '_non_existing1', new_key.account_id,
                new_key.account_id + '_non_existing2'
            ],
        },
        {
            "block_id":
                block_hash,
            "changes_type":
                "single_access_key_changes",
            "keys": [{
                "account_id": new_key.account_id,
                "public_key": new_key.pk
            }],
        },
        {
            "block_id":
                block_hash,
            "changes_type":
                "single_access_key_changes",
            "keys": [
                {
                    "account_id": new_key.account_id + '_non_existing1',
                    "public_key": new_key.pk
                },
                {
                    "account_id": new_key.account_id,
                    "public_key": new_key.pk
                },
            ],
        },
    ]:
        assert_changes_response(request=request,
                                expected_response=expected_response)


def test_key_value_changes():
    """
    Plan:
    1. Deploy a contract.
    2. Observe the code changes in the block where the transaction outcome "lands".
    3. Send two transactions to be included into the same block setting and overriding the value of
       the same key (`my_key`).
    4. Observe the changes in the block where the transaction outcome "lands".
    """

    contract_key = nodes[0].signer_key
    hello_smart_contract = load_binary_file('../tests/hello.wasm')

    # Step 1
    status = nodes[0].get_status()
    latest_block_hash = status['sync_info']['latest_block_hash']
    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        contract_key, hello_smart_contract, 10,
        base58.b58decode(latest_block_hash.encode('utf8')))
    deploy_contract_response = nodes[0].send_tx_and_wait(deploy_contract_tx, 10)

    # Step 2
    block_hash = deploy_contract_response['result']['transaction_outcome'][
        'block_hash']
    assert_changes_in_block_response(
        request={"block_id": block_hash},
        expected_response={
            "block_hash":
                block_hash,
            "changes": [{
                "type": "account_touched",
                "account_id": contract_key.account_id,
            }, {
                "type": "contract_code_touched",
                "account_id": contract_key.account_id,
            }, {
                "type": "access_key_touched",
                "account_id": contract_key.account_id,
            }]
        })

    base_request = {
        "block_id": block_hash,
        "changes_type": "contract_code_changes",
    }
    for request in [
            # Test empty account_ids
        {
            **base_request, "account_ids": []
        },
            # Test an account_id that is a prefix of the original account_id
        {
            **base_request, "account_ids": [contract_key.account_id[:-1]]
        },
            # Test an account_id that has the original account_id as a prefix
        {
            **base_request, "account_ids": [contract_key.account_id + '_extra']
        },
    ]:
        assert_changes_response(request=request,
                                expected_response={
                                    "block_hash": block_hash,
                                    "changes": []
                                })

    # Test happy-path
    expected_response = {
        "block_hash":
            block_hash,
        "changes": [{
            "cause": {
                "type":
                    "receipt_processing",
                "receipt_hash":
                    deploy_contract_response["result"]["receipts_outcome"][0]
                    ["id"],
            },
            "type": "contract_code_update",
            "change": {
                "account_id":
                    contract_key.account_id,
                "code_base64":
                    base64.b64encode(hello_smart_contract).decode('utf-8'),
            }
        },]
    }
    base_request = {
        "block_id": block_hash,
        "changes_type": "contract_code_changes",
    }
    for request in [
        {
            **base_request, "account_ids": [contract_key.account_id]
        },
        {
            **base_request, "account_ids": [
                contract_key.account_id + '_non_existing1',
                contract_key.account_id,
                contract_key.account_id + '_non_existing2'
            ]
        },
    ]:
        assert_changes_response(request=request,
                                expected_response=expected_response)

    # Step 3
    status = nodes[1].get_status()
    latest_block_hash = status['sync_info']['latest_block_hash']
    function_caller_key = nodes[0].signer_key

    def set_value_1():
        function_call_1_tx = transaction.sign_function_call_tx(
            function_caller_key, contract_key.account_id, 'setKeyValue',
            json.dumps({
                "key": "my_key",
                "value": "my_value_1"
            }).encode('utf-8'), 300000000000000, 100000000000, 20,
            base58.b58decode(latest_block_hash.encode('utf8')))
        nodes[1].send_tx_and_wait(function_call_1_tx, 10)

    function_call_1_thread = threading.Thread(target=set_value_1)
    function_call_1_thread.start()

    function_call_2_tx = transaction.sign_function_call_tx(
        function_caller_key, contract_key.account_id, 'setKeyValue',
        json.dumps({
            "key": "my_key",
            "value": "my_value_2"
        }).encode('utf-8'), 300000000000000, 100000000000, 30,
        base58.b58decode(latest_block_hash.encode('utf8')))
    function_call_2_response = nodes[1].send_tx_and_wait(function_call_2_tx, 10)
    assert function_call_2_response['result']['receipts_outcome'][0]['outcome']['status'] == {'SuccessValue': ''}, \
        "Expected successful execution, but the output was: %s" % function_call_2_response
    function_call_1_thread.join()

    tx_block_hash = function_call_2_response['result']['transaction_outcome'][
        'block_hash']

    # Step 4
    assert_changes_in_block_response(
        request={"block_id": tx_block_hash},
        expected_response={
            "block_hash":
                tx_block_hash,
            "changes": [
                {
                    "type": "account_touched",
                    "account_id": contract_key.account_id,
                },
                {
                    "type": "access_key_touched",
                    "account_id": contract_key.account_id,
                },
                {
                    "type": "data_touched",
                    "account_id": contract_key.account_id,
                },
            ]
        })

    base_request = {
        "block_id": block_hash,
        "changes_type": "data_changes",
        "key_prefix_base64": base64.b64encode(b"my_key").decode('utf-8'),
    }
    for request in [
            # Test empty account_ids
        {
            **base_request, "account_ids": []
        },
            # Test an account_id that is a prefix of the original account_id
        {
            **base_request, "account_ids": [contract_key.account_id[:-1]]
        },
            # Test an account_id that has the original account_id as a prefix
        {
            **base_request, "account_ids": [contract_key.account_id + '_extra']
        },
            # Test non-existing key prefix
        {
            **base_request,
            "account_ids": [contract_key.account_id],
            "key_prefix_base64":
                base64.b64encode(b"my_key_with_extra").decode('utf-8'),
        },
    ]:
        assert_changes_response(request=request,
                                expected_response={
                                    "block_hash": block_hash,
                                    "changes": []
                                })

    # Test happy-path
    expected_response = {
        "block_hash":
            tx_block_hash,
        "changes": [{
            "cause": {
                "type": "receipt_processing",
            },
            "type": "data_update",
            "change": {
                "account_id": contract_key.account_id,
                "key_base64": base64.b64encode(b"my_key").decode('utf-8'),
                "value_base64": base64.b64encode(b"my_value_1").decode('utf-8'),
            }
        }, {
            "cause": {
                "type":
                    "receipt_processing",
                "receipt_hash":
                    function_call_2_response["result"]["receipts_outcome"][0]
                    ["id"],
            },
            "type": "data_update",
            "change": {
                "account_id": contract_key.account_id,
                "key_base64": base64.b64encode(b"my_key").decode('utf-8'),
                "value_base64": base64.b64encode(b"my_value_2").decode('utf-8'),
            }
        }]
    }

    base_request = {
        "block_id": tx_block_hash,
        "changes_type": "data_changes",
        "key_prefix_base64": base64.b64encode(b"my_key").decode('utf-8'),
    }
    for request in [
        {
            **base_request, "account_ids": [contract_key.account_id]
        },
        {
            **base_request, "account_ids": [
                contract_key.account_id + '_non_existing1',
                contract_key.account_id,
                contract_key.account_id + '_non_existing2'
            ]
        },
        {
            **base_request,
            "account_ids": [contract_key.account_id],
            "key_prefix_base64": base64.b64encode(b"").decode('utf-8'),
        },
        {
            **base_request,
            "account_ids": [contract_key.account_id],
            "key_prefix_base64": base64.b64encode(b"my_ke").decode('utf-8'),
        },
    ]:
        assert_changes_response(
            request=request,
            expected_response=expected_response,
            exclude_paths={"root['changes'][0]['cause']['receipt_hash']"},
        )


if __name__ == '__main__':
    test_changes_with_new_account_with_access_key()
    test_key_value_changes()
