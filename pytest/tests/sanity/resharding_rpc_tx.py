#!/usr/bin/env python3
"""
Testing RPC call to transaction status after a resharding event.
We create two account that we know would fall into different shards after resharding.
We submit a transfer transaction between the accounts and verify the transaction status after resharding.
"""

import sys
import unittest
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
from resharding_lib import get_target_shard_layout_version, get_target_num_shards, get_genesis_config_changes, get_client_config_changes
import transaction
from utils import MetricsTracker, poll_blocks
import key

STARTING_AMOUNT = 123 * (10**24)


class ReshardingRpcTx(unittest.TestCase):

    def setUp(self) -> None:
        self.epoch_length = 5
        self.config = cluster.load_config()
        self.binary_protocol_version = cluster.get_binary_protocol_version(
            self.config)
        assert self.binary_protocol_version is not None

        self.target_shard_layout_version = get_target_shard_layout_version(
            self.binary_protocol_version)
        self.target_num_shards = get_target_num_shards(
            self.binary_protocol_version)

    def __setup_account(self, account_id, nonce):
        """ Create an account with full access key and balance. """
        encoded_block_hash = self.node.get_latest_block().hash_bytes
        account = key.Key.from_random(account_id)
        account_tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
            self.node.signer_key, account.account_id, account, STARTING_AMOUNT,
            nonce, encoded_block_hash)
        response = self.node.send_tx_and_wait(account_tx, timeout=20)
        assert 'error' not in response, response
        assert 'Failure' not in response['result']['status'], response
        return account

    def __submit_transfer_tx(self, from_key, to_account_id, nonce):
        """ Submit a transfer transaction and wait for the response. """
        encoded_block_hash = self.node.get_latest_block().hash_bytes
        payment_tx = transaction.sign_payment_tx(from_key, to_account_id, 100,
                                                 nonce, encoded_block_hash)
        response = self.node.send_tx_and_wait(payment_tx, timeout=20)
        assert 'error' not in response, response
        assert 'Failure' not in response['result']['status'], response
        return response

    def __verify_tx_status(self, transfer_response, sender_account_id):
        tx_hash = transfer_response['result']['transaction']['hash']
        response = self.node.get_tx(tx_hash, sender_account_id)
        assert response == transfer_response, response
        pass

    def test_resharding_rpc_tx(self):
        num_nodes = 2
        genesis_config_changes = get_genesis_config_changes(
            self.epoch_length, self.binary_protocol_version, logger)
        client_config_changes = get_client_config_changes(num_nodes)
        nodes = cluster.start_cluster(
            num_nodes=num_nodes,
            num_observers=0,
            num_shards=1,
            config=self.config,
            genesis_config_changes=genesis_config_changes,
            client_config_changes=client_config_changes)
        self.node = nodes[0]

        # The shard boundaries are at "kkuuue2akv_1630967379.near" and "tge-lockup.sweat" for shard 3 and 4
        # We would like to create accounts that are in different shards
        # The first account before and after resharding is in shard 3
        # The second account after resharding is in shard 4
        account0 = self.__setup_account('setup_test_account.test0', 1)
        account1 = self.__setup_account('z_setup_test_account.test0', 2)

        # Poll one block to create the accounts
        poll_blocks(self.node)

        # Submit a transfer transaction between the accounts, we would verify the transaction status later
        response0 = self.__submit_transfer_tx(account0, account1.account_id,
                                              6000001)
        response1 = self.__submit_transfer_tx(account1, account0.account_id,
                                              12000001)

        metrics_tracker = MetricsTracker(self.node)
        for height, _ in poll_blocks(self.node):
            # wait for resharding to complete
            if height < 2 * self.epoch_length:
                continue

            # Quick check whether resharding is completed
            version = metrics_tracker.get_int_metric_value(
                "near_shard_layout_version")
            num_shards = metrics_tracker.get_int_metric_value(
                "near_shard_layout_num_shards")
            self.assertEqual(version, self.target_shard_layout_version)
            self.assertEqual(num_shards, self.target_num_shards)

            # Verify the transaction status after resharding
            self.__verify_tx_status(response0, account0.account_id)
            self.__verify_tx_status(response0, account1.account_id)
            self.__verify_tx_status(response1, account0.account_id)
            self.__verify_tx_status(response1, account1.account_id)
            break


if __name__ == '__main__':
    unittest.main()
