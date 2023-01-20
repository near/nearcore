#!/usr/bin/env python3
# The test launches two validating node out of three validators.
# Transfer some tokens between two accounts (thus changing state).
# Query for no finality, doomslug finality
# Nov 2021 - the test was fixed, but it now check for both finalities.
# We might want to update it in the future in order to be able to 'exactly' find
# the moment when doomslug is there, but finality is not.

import sys, time, base58
import pathlib

import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_payment_tx


class TestRpcFinality(unittest.TestCase):

    def test_finality(self):
        nodes = start_cluster(4, 1, 1, None,
                              [["min_gas_price", 0], ["epoch_length", 100]], {})

        time.sleep(3)
        # kill one validating node so that no block can be finalized
        nodes[2].kill()
        time.sleep(1)

        acc0_balance = int(nodes[0].get_account('test0')['result']['amount'])
        acc1_balance = int(nodes[0].get_account('test1')['result']['amount'])

        token_transfer = 10
        latest_block_hash = nodes[0].get_latest_block().hash_bytes
        tx = sign_payment_tx(nodes[0].signer_key, 'test1', token_transfer, 1,
                             latest_block_hash)
        logger.info("About to send payment")
        logger.info(nodes[0].send_tx_and_wait(tx, timeout=200))
        logger.info("Done")

        # wait for doomslug finality
        time.sleep(5)
        for i in range(2):
            acc_id = 'test0' if i == 0 else 'test1'
            acc_no_finality = nodes[0].get_account(acc_id)
            acc_doomslug_finality = nodes[0].get_account(acc_id, "near-final")
            acc_nfg_finality = nodes[0].get_account(acc_id, "final")
            if i == 0:
                self.assertEqual(int(acc_no_finality['result']['amount']),
                                 acc0_balance - token_transfer)
                self.assertEqual(int(acc_doomslug_finality['result']['amount']),
                                 acc0_balance - token_transfer)
                self.assertEqual(int(acc_nfg_finality['result']['amount']),
                                 acc0_balance - token_transfer)
            else:
                self.assertEqual(int(acc_no_finality['result']['amount']),
                                 acc1_balance + token_transfer)
                self.assertEqual(int(acc_doomslug_finality['result']['amount']),
                                 acc1_balance + token_transfer)
                self.assertEqual(int(acc_nfg_finality['result']['amount']),
                                 acc1_balance + token_transfer)


if __name__ == '__main__':
    unittest.main()
