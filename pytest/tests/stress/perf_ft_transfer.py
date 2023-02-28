#!/usr/bin/env python3

"""
This is a benchmark in which a network with a single fungible_token contract is
deployed, and then a variable number of users (see `N_ACCOUNTS`) send each other
them fungible tokens.

At the time this benchmark is written, the intent is to observe the node metrics
and traces for the block duration, potentially adding any additional
instrumentation as needed.

To run:

```
env NEAR_ROOT=../target/release/ FUNGIBLE_TOKEN_WASM=$HOME/FT/res/fungible_token.wasm \
    python3 tests/stress/perf_ft_transfer.py
```
"""

# TODO: add code for to randomly select account pairs and send FT between them
# TODO: add code to set up a network of nodes on GCP (see `GcpNode`)
# TODO: see also pytest/tests/loadtest/loadtest.py

import sys
import os
import time
import pathlib
import base58
import itertools

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
import utils
import account
import transaction
import key
from configured_logger import logger

N_ACCOUNTS = 10000

def main():
    config = cluster.load_config()
    nonce = 1
    nodes = cluster.start_cluster(2, 0, 4, config, [["epoch_length", 100]], {
        0: {
            "tracked_shards": [0, 1, 2, 3],
        }
    })

    contract_path = os.environ["FUNGIBLE_TOKEN_WASM"]
    contract_bytes = pathlib.Path(contract_path).read_bytes()
    block_hash = nodes[0].get_latest_block().hash
    block_hash = base58.b58decode(block_hash)
    deploy_tx = transaction.sign_deploy_contract_tx(nodes[0].signer_key, contract_bytes, nonce, block_hash)
    deploy_res = nodes[0].send_tx_and_wait(deploy_tx, 10**9)
    nonce += 1
    print(f'ft deployment {deploy_res}')

    block_hash = nodes[0].get_latest_block().hash
    block_hash = base58.b58decode(block_hash)
    s = f'{{"owner_id": "{nodes[0].signer_key.account_id}", "total_supply": "{10**33}"}}'
    init_call_tx = transaction.sign_function_call_tx(nodes[0].signer_key, "test0", "new_default_meta", s.encode('utf-8'), int(300E12), 0, nonce, block_hash)
    init_res = nodes[0].send_tx_and_wait(init_call_tx, 10**9)
    print(f'ft new_default_meta {init_res}')

    accounts = []
    for i in range(N_ACCOUNTS):
        signer_key = nodes[0].signer_key
        account = key.Key.implicit_account()
        accounts.append(account)

    accounts_to_create = set(accounts)

    while accounts_to_create:
        waitlist = dict()
        while accounts_to_create:
            account = accounts_to_create.pop()
            print(f"Will attempt to create {account.account_id}")
            block_hash = nodes[0].get_latest_block().hash
            block_hash = base58.b58decode(block_hash)
            tx = transaction.sign_payment_tx(nodes[0].signer_key, account.account_id, 10**24, nonce, block_hash)
            result = nodes[0].send_tx(tx)
            nonce += 1
            waitlist[result["result"]] = account

        for height, hsh in itertools.islice(utils.poll_blocks(nodes[0], timeout=10**9), 10):
            new_waitlist = dict()
            if not waitlist:
                break
            while waitlist:
                k, v = waitlist.popitem()
                tx_result = nodes[0].get_tx(k, v.account_id)
                if 'error' in tx_result:
                    new_waitlist[k] = v
                else:
                    print(f"{v.account_id} was created in {k}")
            waitlist = new_waitlist
        accounts_to_create.update(waitlist.values())

if __name__ == "__main__":
    main()
