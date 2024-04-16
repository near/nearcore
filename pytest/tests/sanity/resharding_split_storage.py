#!/usr/bin/env python3

# Testing the resharding and split storage logic. The goal is to make sure that
# all of the data is correctly moved to the cold storage during and after resharding.
# Usage:
# python3 pytest/tests/sanity/resharding.py
# RUST_LOG=info,resharding=debug,sync=debug,catchup=debug python3 pytest/tests/sanity/resharding_split_storage.py

import copy
import pathlib
import string
import sys
import random
import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import get_config_json, init_cluster, set_config_json, spin_up_node
from utils import MetricsTracker, poll_blocks
from resharding_lib import ReshardingTestBase, get_genesis_config_changes, get_client_config_changes
import key
import transaction

# STARTING_AMOUNT = 123 * (10**24)
STARTING_AMOUNT = 123 * (10**9)

CREATE_COUNT = 1000
DELETE_COUNT = 500


class ReshardingTest(ReshardingTestBase):

    def setUp(self) -> None:
        super().setUp(epoch_length=20)

    def test_resharding(self):
        logger.info("The resharding test is starting.")
        num_nodes = 2

        genesis_config_changes = get_genesis_config_changes(
            self.epoch_length, self.binary_protocol_version, logger)
        client_config_changes = get_client_config_changes(num_nodes)

        client_config_changes[1]['archive'] = True
        client_config_changes[1]['save_trie_changes'] = True
        client_config_changes[1]['split_storage'] = {
            'enable_split_storage_view_client': True
        }

        near_root, [rpc_dir, archival_dir] = init_cluster(
            num_nodes=num_nodes,
            num_observers=0,
            num_shards=1,
            config=self.config,
            genesis_config_changes=genesis_config_changes,
            client_config_changes=client_config_changes,
        )

        self.__configure_cold_storage(archival_dir)

        rpc_node = spin_up_node(
            self.config,
            near_root,
            rpc_dir,
            0,
        )
        archival_node = spin_up_node(
            self.config,
            near_root,
            archival_dir,
            1,
            boot_node=rpc_node,
        )

        metrics_tracker = MetricsTracker(archival_node)

        accounts = []

        for i in range(CREATE_COUNT):
            account_id = gen_account_id()
            account = self.__create_account(rpc_node, account_id, i)
            accounts.append(account)

        for height, hash in poll_blocks(archival_node):
            version = self.get_version(metrics_tracker)
            num_shards = self.get_num_shards(metrics_tracker)

            logger.info(
                f"#{height} shard layout version: {version}, num shards: {num_shards}"
            )

            self.check_protocol_config(
                archival_node,
                height,
                hash,
                version,
                num_shards,
            )

            # This may be flaky - it shouldn't - but it may. We collect metrics
            # after the block is processed. If there is some delay the shard
            # layout may change and the assertions below will fail.

            # TODO(resharding) Why is epoch offset needed here?
            if height <= 2 * self.epoch_length + self.epoch_offset:
                self.assertEqual(version, self.genesis_shard_layout_version)
                self.assertEqual(num_shards, self.genesis_num_shards)
            else:
                self.assertEqual(version, self.target_shard_layout_version)
                self.assertEqual(num_shards, self.target_num_shards)

            if height >= 2.5 * self.epoch_length:
                break

        latest_block_hash = rpc_node.get_latest_block().hash_bytes
        nonce = height
        for account in random.sample(accounts, DELETE_COUNT):
            logger.info(f"deleting account {account.account_id}")
            nonce += 1
            self.__delete_account(
                rpc_node,
                account,
                account.account_id,
                "near",
                nonce,
                latest_block_hash,
            )

        # Run for enough epochs to trigger gc.
        for height, hash in poll_blocks(archival_node):
            if height % self.epoch_length == 0:
                result = archival_node.json_rpc(
                    method="query",
                    params={
                        "request_type": "view_account",
                        "block_id": hash,
                        "account_id": "near"
                    },
                )
                result = result.get("result")
                logger.info(f"{height} {result}")

            if height >= 10 * self.epoch_length:
                break

        logger.info(
            f"checking the historical state at the first block of the new shard layout"
        )
        for account in accounts:
            result = archival_node.json_rpc(
                method="query",
                params={
                    "request_type": "view_account",
                    "block_id": 2 * self.epoch_length + self.epoch_offset + 1,
                    "account_id": account.account_id
                },
            )
            if result is None:
                logger.info(f"result is none for {account.account_id} ")
            else:
                if result is not None and 'result' in result:
                    logger.info(f"{result.get('result')}")
                if result is not None and 'error' in result:
                    logger.info(f"{result.get('error')}")

        rpc_node.kill()
        archival_node.kill()

        logger.info("The resharding test is finished.")

    def check_protocol_config(
        self,
        node,
        height,
        hash,
        version,
        num_shards,
    ):
        protocol_config = node.json_rpc(
            "EXPERIMENTAL_protocol_config",
            {"block_id": hash},
        )

        self.assertTrue('error' not in protocol_config)

        self.assertTrue('result' in protocol_config)
        protocol_config = protocol_config.get('result')

        self.assertTrue('shard_layout' in protocol_config)
        shard_layout = protocol_config.get('shard_layout')

        self.assertTrue('V1' in shard_layout)
        shard_layout = shard_layout.get('V1')

        self.assertTrue('boundary_accounts' in shard_layout)
        boundary_accounts = shard_layout.get('boundary_accounts')

        logger.debug(f"#{height} shard layout: {shard_layout}")

        # check the shard layout versions from metrics and from json rpc are equal
        self.assertEqual(version, shard_layout.get('version'))
        # check the shard num from metrics and json rpc are equal
        self.assertEqual(num_shards, len(boundary_accounts) + 1)

    def __configure_cold_storage(self, node_dir):
        node_config = get_config_json(node_dir)

        node_config["archive"] = True
        node_config["save_trie_changes"] = True
        node_config["enable_split_storage_view_client"] = True

        # Need to create a deepcopy of the store config, otherwise
        # store and cold store will point to the same instance and
        # both will end up with the same path.
        node_config["cold_store"] = copy.deepcopy(node_config["store"])

        # just to speed up the test
        node_config["gc_num_epochs_to_keep"] = 3

        set_config_json(node_dir, node_config)

    def __create_account(self, node, account_id, nonce):
        """ Create an account with full access key and balance. """
        encoded_block_hash = node.get_latest_block().hash_bytes
        account = key.Key.from_random(account_id)
        account_tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
            node.signer_key,
            account.account_id,
            account,
            STARTING_AMOUNT + nonce,
            nonce,
            encoded_block_hash,
        )
        response = node.send_tx(account_tx)
        print(response)
        assert 'error' not in response, response
        assert 'result' in response, response
        return account

    def __delete_account(
        self,
        node,
        key,
        account_id,
        beneficiary,
        nonce,
        latest_block_hash,
    ):
        tx = transaction.sign_delete_account_tx(
            key,
            account_id,
            beneficiary,
            nonce,
            latest_block_hash,
        )
        response = node.send_tx(tx)
        assert 'error' not in response, response
        assert 'result' in response, response

    def __transfer(self, node, from_key, to_account_id, nonce):
        logger.info(f"submit transfer tx from {from_key} to {to_account_id}")
        """ Submit a transfer transaction and wait for the response. """
        encoded_block_hash = node.get_latest_block().hash_bytes
        payment_tx = transaction.sign_payment_tx(
            from_key,
            to_account_id,
            100,
            nonce,
            encoded_block_hash,
        )
        response = node.send_tx(payment_tx)
        assert 'error' not in response, response
        assert 'result' in response, response
        return response


def gen_account_id():
    len = random.randint(1, 5)
    rand = ''.join(random.choices(string.ascii_lowercase, k=len))
    return "ga" + rand + ".test0"


if __name__ == '__main__':
    unittest.main()
