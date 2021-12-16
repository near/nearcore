#!/usr/bin/env python3
# Experiments with deploying gibberish contracts. Specifically,
# 1. Deploys completely gibberish contracts
# 2. Gets an existing wasm contract, and tries to arbitrarily pertrurb bytes in it

import sys, time, random
import base58
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import start_cluster
from configured_logger import logger
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_test_contract

nodes = start_cluster(
    3, 0, 4, None,
    [["epoch_length", 1000], ["block_producer_kickout_threshold", 80]], {})

wasm_blob_1 = load_test_contract()

hash_ = nodes[0].get_latest_block().hash_bytes

for iter_ in range(10):
    logger.info("Deploying garbage contract #%s" % iter_)
    wasm_blob = bytes(
        [random.randint(0, 255) for _ in range(random.randint(200, 500))])
    tx = sign_deploy_contract_tx(nodes[0].signer_key, wasm_blob, 10 + iter_,
                                 hash_)
    nodes[0].send_tx_and_wait(tx, 20)

for iter_ in range(10):
    hash_ = nodes[0].get_latest_block().hash_bytes
    logger.info("Deploying perturbed contract #%s" % iter_)

    new_name = '%s_mething' % iter_
    new_output = '%s_llo' % iter_

    wasm_blob = wasm_blob_1.replace(bytes('something', 'utf8'),
                                    bytes(new_name, 'utf8')).replace(
                                        bytes('hello', 'utf8'),
                                        bytes(new_output, 'utf8'))
    assert len(wasm_blob) == len(wasm_blob_1)

    pos = random.randint(0, len(wasm_blob_1) - 1)
    val = random.randint(0, 255)
    wasm_blob = wasm_blob[:pos] + bytes([val]) + wasm_blob[pos + 1:]
    tx = sign_deploy_contract_tx(nodes[0].signer_key, wasm_blob, 20 + iter_ * 2,
                                 hash_)
    res = nodes[0].send_tx_and_wait(tx, 20)
    assert 'result' in res
    logger.info(res)

    logger.info("Invoking perturbed contract #%s" % iter_)

    tx2 = sign_function_call_tx(nodes[0].signer_key,
                                nodes[0].signer_key.account_id, new_name, [],
                                3000000000000, 100000000000, 20 + iter_ * 2 + 1,
                                hash_)
    # don't have any particular expectation for the call result, but the transaction should at least go through
    res = nodes[1].send_tx_and_wait(tx2, 20)
    assert 'result' in res

hash_ = nodes[0].get_latest_block().hash_bytes

logger.info("Real thing!")
tx = sign_deploy_contract_tx(nodes[0].signer_key, wasm_blob_1, 60, hash_)
nodes[0].send_tx(tx)

time.sleep(3)

hash_2 = nodes[1].get_latest_block().hash_bytes
tx2 = sign_function_call_tx(nodes[0].signer_key, nodes[0].signer_key.account_id,
                            'log_something', [], 3000000000000, 100000000000,
                            62, hash_2)
res = nodes[1].send_tx_and_wait(tx2, 20)
logger.info(res)
assert res['result']['receipts_outcome'][0]['outcome']['logs'][0] == 'hello'
