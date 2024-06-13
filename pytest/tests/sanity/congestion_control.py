#!/usr/bin/env python3
# This is a test for congestion control. It spins up a single node and loads it
# with heavy transactions to congest one of the shards. It then checks that the
# congestion info is correct and that the chain rejected some transactions.
# Usage:
# python3 pytest/tests/sanity/congestion_control.py

import unittest
import pathlib
import sys
import json
import time
import threading

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
from cluster import BaseNode, init_cluster, load_config, spin_up_node, Key
from utils import load_test_contract, poll_blocks, wait_for_blocks
from transaction import sign_create_account_with_full_access_key_and_balance_tx, sign_deploy_contract_tx, sign_function_call_tx

ONE_NEAR = 10**24
TGAS = 10**12

ACCESS_KEY_NONCE_RANGE_MULTIPLIER = 1_000_000

GOOD_FINAL_EXECUTION_STATUS = ['FINAL', 'EXECUTED', 'EXECUTED_OPTIMISTIC']

# Shard layout with 5 roughly equal size shards for convenience.
SHARD_LAYOUT = {
    "V1": {
        "boundary_accounts": [
            "fff",
            "kkk",
            "ppp",
            "uuu",
        ],
        "version": 2,
        "shards_split_map": [],
        "to_parent_shard_map": [],
    }
}

NUM_SHARDS = len(SHARD_LAYOUT["V1"]["boundary_accounts"]) + 1

ACCOUNT_SHARD_0 = "aaa.test0"
ACCOUNT_SHARD_1 = "ggg.test0"
ACCOUNT_SHARD_2 = "lll.test0"
ACCOUNT_SHARD_3 = "rrr.test0"
ACCOUNT_SHARD_4 = "vvv.test0"

ALL_ACCOUNTS = [
    ACCOUNT_SHARD_0,
    ACCOUNT_SHARD_1,
    ACCOUNT_SHARD_2,
    ACCOUNT_SHARD_3,
    ACCOUNT_SHARD_4,
]

TxHash = str
AccountId = str


class CongestionControlTest(unittest.TestCase):

    def setUp(self):
        self.threads = []

    def tearDown(self):
        self.__stop_load()

    def test(self):

        self.nonce = 1

        accounts = self.__prepare_accounts()

        node = self.__setup_node()

        self.__create_accounts(node, accounts)

        self.__deploy_contracts(node, accounts)

        self.__run(node, accounts)

        node.kill()

        logger.info("The test is finished.")

    def __run(self, node, accounts):

        self.__start_load(node, accounts)

        self.__run_under_congestion(node)

        self.__stop_load()

        self.__run_after_congestion(node)

        self.__check_txs(node)

    def __run_under_congestion(self, node):
        logger.info("Checking the chain under congestion")
        (start_height, _) = node.get_latest_block()
        for height, hash in poll_blocks(node, __target=30):
            # Wait for a few blocks to congest the chain.
            if height < start_height + 5:
                continue

            # Check the target shard.

            chunk = self.__get_chunk(node, hash, 0)

            # Check that the target is busy - always using 1000TG.
            gas_used = chunk['header']['gas_used']
            self.assertGreaterEqual(gas_used, 1000 * TGAS)

            # Check that the congestion info has no buffered receipts and some delayed receipts.
            congestion_info = chunk['header']['congestion_info']
            self.assertEqual(int(congestion_info['buffered_receipts_gas']), 0)
            self.assertGreater(int(congestion_info['delayed_receipts_gas']), 0)
            self.assertGreater(congestion_info['receipt_bytes'], 0)

            logger.info(
                f"#{height} target gas used: {gas_used} congestion info {congestion_info}"
            )

            # Check one other shard.

            chunk = self.__get_chunk(node, hash, 1)
            gas_used = chunk['header']['gas_used']
            congestion_info = chunk['header']['congestion_info']

            self.assertEqual(int(congestion_info['buffered_receipts_gas']), 0)
            self.assertEqual(int(congestion_info['delayed_receipts_gas']), 0)
            self.assertEqual(congestion_info['receipt_bytes'], 0)

            logger.info(
                f"#{height} other gas used: {gas_used} congestion info {congestion_info}"
            )

    def __run_after_congestion(self, node):
        logger.info("Checking the chain after congestion")
        for height, hash in poll_blocks(node, __target=50):
            chunk = self.__get_chunk(node, hash, 0)

            gas_used = chunk['header']['gas_used']
            congestion_info = chunk['header']['congestion_info']

            logger.info(
                f"#{height} gas used: {gas_used} congestion info {congestion_info}"
            )

        chunk = self.__get_chunk(node, hash, 0)
        gas_used = chunk['header']['gas_used']
        congestion_info = chunk['header']['congestion_info']

        self.assertEqual(gas_used, 0)
        self.assertEqual(int(congestion_info['buffered_receipts_gas']), 0)
        self.assertEqual(int(congestion_info['delayed_receipts_gas']), 0)
        self.assertEqual(congestion_info['receipt_bytes'], 0)

    def __check_txs(self, node: BaseNode):
        logger.info("Checking transactions. This is slow.")

        accepted_count = 0
        rejected_count = 0
        total = len(self.txs)
        checked = 0
        for (tx_sender, tx_hash) in self.txs:
            try:
                # The transactions should be long done by now. Set a short
                # timeout to speed up the test.
                # TODO It would be better to check the transactions in parallel.
                result = node.get_tx(tx_hash, tx_sender, timeout=1)

                status = result['result']['final_execution_status']
                self.assertIn(status, GOOD_FINAL_EXECUTION_STATUS)

                status = result['result']['status']
                self.assertIn('SuccessValue', status)

                accepted_count += 1
            except:
                rejected_count += 1

            checked += 1
            if checked % 10 == 0:
                logger.info(
                    f"Checking transactions under way, {checked}/{total}")

        logger.info(
            f"Checking transactions done, total {len(self.txs)}, accepted {accepted_count}, rejected {rejected_count}"
        )
        self.assertGreater(accepted_count, 0)
        self.assertGreater(rejected_count, 0)

    def __start_load(self, node: BaseNode, accounts):
        logger.info("Starting load threads")
        self.finished = False
        self.lock = threading.Lock()

        target_account = accounts[0]
        for account in accounts:
            thread = threading.Thread(
                target=self.__load,
                args=[node, account, target_account],
            )
            self.threads.append(thread)

        for thread in self.threads:
            thread.start()

    def __stop_load(self):
        logger.info("Stopping load threads")
        self.finished = True
        for thread in self.threads:
            thread.join()

    def __load(self, node: BaseNode, sender_account, target_account):
        logger.debug(
            f"Starting load thread {sender_account.account_id} -> {target_account.account_id}"
        )
        self.txs = []
        while not self.finished:
            tx_hash = self.__call_contract(node, sender_account, target_account)
            with self.lock:
                self.txs.append((sender_account.account_id, tx_hash))

            # This sleep here is more a formality, the call_contract call is
            # slower. This is also the reason for sending transactions from
            # multiple threads.
            time.sleep(0.1)

        logger.debug(
            f"Stopped load thread {sender_account.account_id} -> {target_account.account_id}"
        )

    def __setup_node(self) -> BaseNode:
        logger.info("Setting up the node")
        epoch_length = 100
        config = load_config()
        genesis_config_changes = [
            ("epoch_length", epoch_length),
            ("shard_layout", SHARD_LAYOUT),
        ]
        client_config_changes = {
            0: {
                "tracked_shards": [0]
            },
        }

        near_root, [node_dir] = init_cluster(
            num_nodes=1,
            num_observers=0,
            num_shards=NUM_SHARDS,
            config=config,
            genesis_config_changes=genesis_config_changes,
            client_config_changes=client_config_changes,
        )

        node = spin_up_node(config, near_root, node_dir, 0)
        return node

    def __prepare_accounts(self):
        logger.info("Preparing accounts")

        accounts = []
        for account_id in ALL_ACCOUNTS:
            account_key = Key.from_random(account_id)
            accounts.append(account_key)
        return accounts

    def __create_accounts(self, node: BaseNode, accounts: list[Key]):
        logger.info("Creating accounts")

        create_account_tx_list = []
        for account in accounts:
            tx_hash = self.__create_account(node, account, 1000 * ONE_NEAR)

            create_account_tx_list.append((node.signer_key.account_id, tx_hash))

        self.__wait_for_txs(node, create_account_tx_list)

    def __deploy_contracts(self, node: BaseNode, accounts: list[Key]):
        logger.info("Deploying contracts")

        deploy_contract_tx_list = list()
        for account_key in accounts:
            tx_hash = self.__deploy_contract(node, account_key)
            deploy_contract_tx_list.append((account_key.account_id, tx_hash))

        self.__wait_for_txs(node, deploy_contract_tx_list)

    def __create_account(self, node: BaseNode, account_key, balance):
        block_hash = node.get_latest_block().hash_bytes
        new_signer_key = Key(
            account_key.account_id,
            account_key.pk,
            account_key.sk,
        )
        create_account_tx = sign_create_account_with_full_access_key_and_balance_tx(
            node.signer_key,
            account_key.account_id,
            new_signer_key,
            balance,
            self.nonce,
            block_hash,
        )
        self.nonce += 1
        result = node.send_tx(create_account_tx)
        self.assertIn('result', result, result)
        logger.debug(f"Create account {account_key.account_id}: {result}")
        return result['result']

    def __deploy_contract(self, node: BaseNode, account_key):
        logger.debug("Deploying contract.")

        block_hash = node.get_latest_block().hash_bytes
        contract = load_test_contract('test_contract_rs.wasm')

        tx = sign_deploy_contract_tx(
            account_key,
            contract,
            self.nonce,
            block_hash,
        )
        self.nonce += 1
        result = node.send_tx(tx)
        self.assertIn('result', result, result)
        return result['result']

    def __call_contract(self, node: BaseNode, sender: Key, receiver: Key):
        logger.debug(
            f"Calling contract. {sender.account_id} -> {receiver.account_id}")

        block_hash = node.get_latest_block().hash_bytes

        gas_amount = 250 * TGAS
        gas_bytes = gas_amount.to_bytes(8, byteorder="little")

        tx = sign_function_call_tx(
            sender,
            receiver.account_id,
            'burn_gas_raw',
            gas_bytes,
            300 * TGAS,
            0,
            self.nonce,
            block_hash,
        )
        self.nonce += 1
        result = node.send_tx(tx)
        self.assertIn('result', result, result)
        return result['result']

    def __wait_for_txs(self, node: BaseNode, tx_list: list[AccountId, TxHash]):
        (height, _) = wait_for_blocks(node, count=3)
        self.nonce = ACCESS_KEY_NONCE_RANGE_MULTIPLIER * height + 1

        for (tx_sender, tx_hash) in tx_list:
            result = node.get_tx(tx_hash, tx_sender)
            self.assertIn('result', result, result)

            status = result['result']['final_execution_status']
            self.assertIn(status, GOOD_FINAL_EXECUTION_STATUS)

            status = result['result']['status']
            self.assertIn('SuccessValue', status)

    def __get_chunk(self, node: BaseNode, block_hash, shard_id):
        result = node.json_rpc("chunk", {
            "block_id": block_hash,
            "shard_id": shard_id
        })
        self.assertIn('result', result, result)
        return result['result']


if __name__ == '__main__':
    unittest.main()
