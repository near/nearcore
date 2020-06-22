# Spins up two nodes, deploy a smart contract to one node,
# Send a transaction to call a contract method. Check that
# the transaction and receipts execution outcome proof for
# light client works

import sys
import base58, base64
import json
import hashlib

sys.path.append('lib')
from cluster import start_cluster, Key
from utils import load_binary_file, compute_merkle_root_from_path
from serializer import BinarySerializer
import transaction
import time
from lightclient import compute_block_hash


class PartialExecutionOutcome:
    pass


class PartialExecutionStatus:
    pass


class Unknown:
    pass


class Failure:
    pass


partial_execution_outcome_schema = dict([
    [
        PartialExecutionOutcome, {
        'kind':
            'struct',
        'fields': [
            ['receipt_ids', [[32]]],
            ['gas_burnt', 'u64'],
            ['tokens_burnt', 'u128'],
            ['status', PartialExecutionStatus],
        ]
    },
    ],
    [
        PartialExecutionStatus, {
        'kind': 'enum',
        'field': 'enum',
        'values': [
            ['unknown', Unknown],
            ['failure', Failure],
            ['successValue', ['u8']],
            ['successReceiptId', [32]],
        ]
    }
    ],
    [
        Unknown, {
        'kind': 'struct',
        'fields': []
    }
    ],
    [
        Failure, {
        'kind': 'struct',
        'fields': []
    }
    ],
])


def serialize_execution_outcome_with_id(outcome, id):
    partial_outcome = PartialExecutionOutcome()
    partial_outcome.receipt_ids = [base58.b58decode(x) for x in outcome['receipt_ids']]
    partial_outcome.gas_burnt = outcome['gas_burnt']
    partial_outcome.tokens_burnt = int(outcome['tokens_burnt'])
    execution_status = PartialExecutionStatus()
    if 'SuccessValue' in outcome['status']:
        execution_status.enum = 'successValue'
        execution_status.successValue = base64.b64decode(outcome['status']['SuccessValue'])
    elif 'SuccessReceiptId' in outcome['status']:
        execution_status.enum = 'successReceiptId'
        execution_status.successReceiptId = base58.b58decode(outcome['status']['SuccessReceiptId'])
    elif 'Failure' in outcome['status']:
        execution_status.enum = 'failure'
        execution_status.failure = Failure()
    elif 'Unknown' in outcome['status']:
        execution_status.enum = 'unknown'
        execution_status.unknown = Unknown
    else:
        assert False, f'status not supported: {outcome["status"]}'
    partial_outcome.status = execution_status
    msg = BinarySerializer(partial_execution_outcome_schema).serialize(partial_outcome)
    partial_outcome_hash = hashlib.sha256(msg).digest()
    outcome_hashes = [partial_outcome_hash]
    for log_entry in outcome['logs']:
        outcome_hashes.append(hashlib.sha256(bytes(log_entry, 'utf-8')).digest())
    res = [base58.b58decode(id)]
    res.extend(outcome_hashes)
    borsh_res = bytearray()
    length = len(res)
    for i in range(4):
        borsh_res.append(length & 255)
        length //= 256
    for hash_result in res:
        borsh_res += bytearray(hash_result)
    return borsh_res


nodes = start_cluster(
    2, 0, 1, None,
    [["epoch_length", 1000], ["block_producer_kickout_threshold", 80]], {}
)

# deploy a smart contract for testing
contract_key = nodes[0].signer_key
hello_smart_contract = load_binary_file('../tests/hello.wasm')

status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
deploy_contract_tx = transaction.sign_deploy_contract_tx(
    contract_key, hello_smart_contract, 10,
    base58.b58decode(latest_block_hash.encode('utf8')))
deploy_contract_response = nodes[0].send_tx_and_wait(deploy_contract_tx, 15)
assert 'error' not in deploy_contract_response, deploy_contract_response


def check_transaction_outcome_proof(should_succeed, nonce):
    status = nodes[1].get_status()
    latest_block_hash = status['sync_info']['latest_block_hash']
    function_caller_key = nodes[0].signer_key
    gas = 300000000000000 if should_succeed else 1000

    function_call_1_tx = transaction.sign_function_call_tx(
        function_caller_key, contract_key.account_id, 'setKeyValue',
        json.dumps({
            "key": "my_key",
            "value": "my_value"
        }).encode('utf-8'), gas, 100000000000, nonce,
        base58.b58decode(latest_block_hash.encode('utf8')))
    function_call_result = nodes[1].send_tx_and_wait(function_call_1_tx, 15)
    assert 'error' not in function_call_result

    status = nodes[0].get_status()
    latest_block_height = status['sync_info']['latest_block_height']

    # wait for finalization
    light_client_request_block_hash = None
    while True:
        status = nodes[0].get_status()
        cur_height = status['sync_info']['latest_block_height']
        if cur_height > latest_block_height + 2 and light_client_request_block_hash is None:
            light_client_request_block_hash = status['sync_info']['latest_block_hash']
        if cur_height > latest_block_height + 7:
            break
        time.sleep(1)

    light_client_block = nodes[0].json_rpc('next_light_client_block', [light_client_request_block_hash])['result']
    light_client_block_hash = compute_block_hash(light_client_block['inner_lite'],
                                                 light_client_block['inner_rest_hash'],
                                                 light_client_block['prev_block_hash']).decode('utf-8')

    queries = [
        {"type": "transaction", "transaction_hash": function_call_result['result']['transaction_outcome']['id'], "sender_id": "test0",
         "light_client_head": light_client_block_hash}
    ]
    outcomes = [(function_call_result['result']['transaction_outcome']['outcome'],
                 function_call_result['result']['transaction_outcome']['id'])]
    for receipt_outcome in function_call_result['result']['receipts_outcome']:
        outcomes.append((receipt_outcome['outcome'], receipt_outcome['id']))
        queries.append({"type": "receipt", "receipt_id": receipt_outcome['id'], "receiver_id": "test0",
                        "light_client_head": light_client_block_hash})

    for query, (outcome, id) in zip(queries, outcomes):
        res = nodes[0].json_rpc('light_client_proof', query, timeout=10)
        assert 'error' not in res, res
        light_client_proof = res['result']
        # check that execution outcome root proof is valid
        execution_outcome_hash = hashlib.sha256(serialize_execution_outcome_with_id(outcome, id)).digest()
        outcome_root = compute_merkle_root_from_path(light_client_proof['outcome_proof']['proof'],
                                                     execution_outcome_hash)
        block_outcome_root = compute_merkle_root_from_path(light_client_proof['outcome_root_proof'],
                                                           hashlib.sha256(outcome_root).digest())
        block = nodes[0].json_rpc('block', {"block_id": light_client_proof['outcome_proof']['block_hash']})
        expected_root = block['result']['header']['outcome_root']
        assert base58.b58decode(
            expected_root) == block_outcome_root, f'expected outcome root {expected_root} actual {base58.b58encode(block_outcome_root)}'
        # check that the light block header is valid
        block_header_lite = light_client_proof['block_header_lite']
        computed_block_hash = compute_block_hash(block_header_lite['inner_lite'], block_header_lite['inner_rest_hash'],
                                                 block_header_lite['prev_block_hash'])
        assert light_client_proof['outcome_proof']['block_hash'] == computed_block_hash.decode(
            'utf-8'), f'expected block hash {light_client_proof["outcome_proof"]["block_hash"]} actual {computed_block_hash}'
        # check that block proof is valid
        block_merkle_root = compute_merkle_root_from_path(light_client_proof['block_proof'],
                                                          light_client_proof['outcome_proof']['block_hash'])
        assert base58.b58decode(light_client_block['inner_lite'][
                                    'block_merkle_root']) == block_merkle_root, f'expected block merkle root {light_client_block["inner_lite"]["block_merkle_root"]} actual {base58.b58encode(block_merkle_root)}'


check_transaction_outcome_proof(True, 20)
check_transaction_outcome_proof(False, 30)
