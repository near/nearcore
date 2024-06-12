#!/usr/bin/env python3
# The test does the token transfer between the accounts, and tries to
# stop the network just in the right moment (so that the block with the refund receipt
# is not finalized).
# This way, we can verify that our json RPC returns correct values for different finality requests.

import sys
import pathlib

import unittest
from typing import List

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
import utils
from cluster import start_cluster, LocalNode
from configured_logger import logger
from transaction import sign_payment_tx


class TestRpcViewHistory(unittest.TestCase):

    def test_rpc_view_history(self):
        min_block_delay = 1

        consensus = {
            "min_block_production_delay": {
                "secs": min_block_delay,
                "nanos": 0,
            },
            "max_block_production_delay": {
                "secs": min_block_delay * 2,
                "nanos": 0,
            },
            "max_block_wait_delay": {
                "secs": min_block_delay * 3,
                "nanos": 0,
            }
        }

        store = {
            "load_mem_tries_for_tracked_shards": True,
        }

        config = {
            node_id: {
                "consensus": consensus,
                "store": store
            } for node_id in range(3)
        }

        nodes: List[LocalNode] = start_cluster(3, 0, 1, None, [
            ["min_gas_price", 0],
            ["epoch_length", 100],
        ], config)

        utils.wait_for_blocks(nodes[0], target=3)

        balances = {
            account: int(nodes[0].get_account('test0')['result']['amount'])
            for account in ['test0', 'test1']
        }

        # we will send a payment. After that, we'll wait for a few more blocks so that
        # the previous state before the payment transaction is definitely older than the
        # final block and is therefore historical state (not present in memtrie or flat
        # storage). Check that the RPC is able to provide access to this historical state.
        token_transfer = 10
        latest_block_hash = nodes[0].get_latest_block().hash_bytes
        tx = sign_payment_tx(nodes[0].signer_key, 'test1', token_transfer, 1,
                             latest_block_hash)
        logger.info("About to send payment")
        logger.info(nodes[0].send_tx_and_wait(tx, timeout=10))
        logger.info("Done")

        block_height = nodes[0].get_latest_block().height
        print(f"Block height is {block_height}")

        utils.wait_for_blocks(nodes[0], target=block_height + 5)

        for acc_id in ['test0', 'test1']:
            # Sanity check that the payment transaction did go through.
            amount_delta = int(nodes[0].get_account(
                acc_id, "final")['result']['amount']) - balances[acc_id]

            if acc_id == 'test0':
                self.assertEqual(-10, amount_delta)
            else:
                self.assertEqual(10, amount_delta)

            # Now check that the RPC will provide historical results.
            historical_amount = int(nodes[0].get_account(
                acc_id, block=1)['result']['amount'])
            self.assertEqual(balances[acc_id], historical_amount)


if __name__ == '__main__':
    unittest.main()
