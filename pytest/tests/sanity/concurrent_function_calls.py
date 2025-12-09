#!/usr/bin/env python3
# Spins up four nodes, deploy an smart contract to one node,
# Call a smart contract method in another node

import sys, time
import base58
import base64
import json
import multiprocessing
import pathlib
import requests

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import start_cluster
from configured_logger import logger
from requests.adapters import HTTPAdapter, Retry
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from utils import load_test_contract

nodes = start_cluster(
    4, 1, 4, None,
    [["epoch_length", 10], ["block_producer_kickout_threshold", 80]],
    {4: {
        "tracked_shards_config": "AllShards"
    }})

# Deploy contract
hash_ = nodes[0].get_latest_block().hash_bytes
tx = sign_deploy_contract_tx(nodes[0].signer_key, load_test_contract(), 10,
                             hash_)
nodes[0].send_tx(tx)

time.sleep(3)

# Write 10 values to storage
for i in range(10):
    hash_ = nodes[1].get_latest_block().hash_bytes
    keyvalue = bytearray(16)
    keyvalue[0] = i
    keyvalue[8] = i
    tx2 = sign_function_call_tx(nodes[0].signer_key,
                                nodes[0].signer_key.account_id,
                                'write_key_value', bytes(keyvalue),
                                10000000000000, 100000000000, 20 + i * 10,
                                hash_)
    res = nodes[1].send_tx(tx2)

time.sleep(3)
acc_id = nodes[0].signer_key.account_id


def process():
    for i in range(100):
        key = bytearray(8)
        key[0] = i % 10
        data = {
            'method': 'query',
            'params': {
                "request_type": "call_function",
                "account_id": acc_id,
                "method_name": "read_value",
                "finality": "optimistic",
                "args_base64": base64.b64encode(bytes(key)).decode("ascii")
            },
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1)

        session.mount('http://', HTTPAdapter(max_retries=retries))

        res = session.post("http://%s:%s" % nodes[4].rpc_addr(),
                           json=data,
                           timeout=2)

        assert res.status_code == 200
        res = json.loads(res.text)
        res = int.from_bytes(res["result"]["result"], byteorder='little')
        assert res == (i % 10)
    logger.info("all done")


ps = [multiprocessing.Process(target=process, args=()) for i in range(6)]
for p in ps:
    p.start()

for p in ps:
    p.join()
