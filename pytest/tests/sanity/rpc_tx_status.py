import sys
import base58
import json

sys.path.append('lib')
from cluster import start_cluster, Key
from configured_logger import logger
from utils import load_binary_file
import transaction


def submit_tx_and_check(node, tx):
    res = node.send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res

    tx_hash = res['result']['transaction']['hash']
    query_res = nodes[0].json_rpc('EXPERIMENTAL_tx_status', [tx_hash, 'test0'])
    assert 'error' not in query_res, query_res
    receipt_id_from_outcomes = set(map(lambda x: x['id'], query_res['result']['receipts_outcome']))
    receipt_id_from_receipts = set(map(lambda x: x['receipt_id'], query_res['result']['receipts']))
    is_local_receipt = res['result']['transaction']['signer_id'] == res['result']['transaction']['receiver_id']
    if is_local_receipt:
        receipt_id_from_outcomes.remove(res['result']['transaction_outcome']['outcome']['receipt_ids'][0])
    assert receipt_id_from_outcomes == receipt_id_from_receipts, f'receipt id from outcomes {receipt_id_from_outcomes}, receipt id from receipts {receipt_id_from_receipts} '


nodes = start_cluster(
    4, 0, 1, None,
    [["epoch_length", 1000], ["block_producer_kickout_threshold", 80], ["transaction_validity_period", 10000]], {})

status = nodes[0].get_status()
block_hash = status['sync_info']['latest_block_hash']
logger.info("1")
payment_tx = transaction.sign_payment_tx(nodes[0].signer_key, 'test1', 100, 1,
                                         base58.b58decode(block_hash.encode('utf8')))
submit_tx_and_check(nodes[0], payment_tx)

logger.info("2")
deploy_contract_tx = transaction.sign_deploy_contract_tx(nodes[0].signer_key, load_binary_file('testdata/hello.wasm'), 2, base58.b58decode(block_hash.encode('utf8')))
submit_tx_and_check(nodes[0], deploy_contract_tx)

logger.info("3")
function_call_tx = transaction.sign_function_call_tx(nodes[0].signer_key, nodes[0].signer_key.account_id, 'setKeyValue', json.dumps({
    "key": "my_key",
    "value": "my_value_1"
}).encode('utf-8'), 300000000000000, 0, 3, base58.b58decode(block_hash.encode('utf8')))
submit_tx_and_check(nodes[0], deploy_contract_tx)
