#!/usr/bin/env python3
import base58
import json
import struct
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import cluster
from utils import load_test_contract
import transaction


def submit_tx_and_check(nodes, node_index, tx):
    node = nodes[node_index]
    res = node.send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res

    tx_hash = res['result']['transaction']['hash']
    query_res = nodes[0].json_rpc('EXPERIMENTAL_tx_status', [tx_hash, 'test0'])
    assert 'error' not in query_res, query_res

    receipt_id_from_outcomes = set(
        outcome['id'] for outcome in query_res['result']['receipts_outcome'])
    receipt_id_from_receipts = set(
        rec['receipt_id'] for rec in query_res['result']['receipts'])

    is_local_receipt = (res['result']['transaction']['signer_id'] ==
                        res['result']['transaction']['receiver_id'])
    if is_local_receipt:
        receipt_id_from_outcomes.remove(
            res['result']['transaction_outcome']['outcome']['receipt_ids'][0])

    assert receipt_id_from_outcomes == receipt_id_from_receipts, (
        f'receipt id from outcomes {receipt_id_from_outcomes}, '
        f'receipt id from receipts {receipt_id_from_receipts}')


def test_tx_status(nodes, *, nonce_offset: int = 0):
    signer_key = nodes[0].signer_key
    encoded_block_hash = nodes[0].get_latest_block().hash_bytes
    payment_tx = transaction.sign_payment_tx(signer_key, 'test1', 100,
                                             nonce_offset + 1,
                                             encoded_block_hash)
    submit_tx_and_check(nodes, 0, payment_tx)

    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        signer_key, load_test_contract(), nonce_offset + 2, encoded_block_hash)
    submit_tx_and_check(nodes, 0, deploy_contract_tx)

    function_call_tx = transaction.sign_function_call_tx(
        signer_key, signer_key.account_id, 'write_key_value',
        struct.pack('<QQ', 42, 24), 300000000000000, 0, nonce_offset + 3,
        encoded_block_hash)
    submit_tx_and_check(nodes, 0, deploy_contract_tx)


def start_cluster(*, archive: bool = False):
    num_nodes = 4
    genesis_changes = [["epoch_length", 1000],
                       ["block_producer_kickout_threshold", 80],
                       ["transaction_validity_period", 10000]]
    config_changes = dict.fromkeys(range(num_nodes), {'archive': archive})
    return cluster.start_cluster(num_nodes=num_nodes,
                                 num_observers=0,
                                 num_shards=1,
                                 config=None,
                                 genesis_config_changes=genesis_changes,
                                 client_config_changes=config_changes)


def main():
    test_tx_status(start_cluster())


if __name__ == '__main__':
    main()
