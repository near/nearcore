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


class TestRpcFinality(unittest.TestCase):

    def test_finality(self):
        # set higher block delay to make test more reliable.
        min_block_delay = 3

        consensus = {
            "consensus": {
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
        }

        config = {node_id: {"consensus": consensus} for node_id in range(3)}

        nodes: List[LocalNode] = start_cluster(3, 0, 1, None, [
            ["min_gas_price", 0],
            ["epoch_length", 100],
        ], config)

        utils.wait_for_blocks(nodes[0], target=3)

        balances = {
            account: int(nodes[0].get_account('test0')['result']['amount'])
            for account in ['test0', 'test1']
        }

        token_transfer = 10
        latest_block_hash = nodes[0].get_latest_block().hash_bytes
        tx = sign_payment_tx(nodes[0].signer_key, 'test1', token_transfer, 1,
                             latest_block_hash)
        logger.info("About to send payment")
        # this transaction will be added to the block (probably around block 5)
        # and the the receipts & transfers will happen in the next block (block 6).
        # This function should return as soon as block 6 arrives in node0.
        logger.info(nodes[0].send_tx_and_wait(tx, timeout=10))
        logger.info("Done")

        # kill one validating node so that block cannot be finalized.
        nodes[2].kill()

        print(
            f"Block height is {nodes[0].get_latest_block().height} (should be 6)"
        )

        # So now the situation is following:
        # Block 6 (head) - has the final receipt (that adds state to test1)
        # Block 5 (doomslug) - has the transaction (so this is the moment when state is removed from test0)
        # Block 4 (final) - has no information about the transaction.

        # So with optimistic finality: test0 = -10, test1 = +10
        # with doomslug (state as of block 5): test0 = -10, test1 = 0
        # with final (state as of block 4): test0 = 0, test1 = 0

        for acc_id in ['test0', 'test1']:
            amounts = [
                int(nodes[0].get_account(acc_id, finality)['result']['amount'])
                - balances[acc_id]
                for finality in ["optimistic", "near-final", "final"]
            ]
            print(f"Account amounts: {acc_id}: {amounts}")

            if acc_id == 'test0':
                self.assertEqual([-10, -10, 0], amounts)
            else:
                self.assertEqual([10, 0, 0], amounts)


if __name__ == '__main__':
    unittest.main()
