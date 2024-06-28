#!/usr/bin/env python3
# Spins up 4 validating nodes and 1 non-validating node. There are four shards in this test.
# Tests the following scenario and checks if the network can progress over a few epochs.
# 1. Starts with memtries enabled.
# 2. Restarts 2 of the validator nodes with memtries disabled.
# 3. Restarts the remaining 2 nodes with memtries disabled.
# Sends random transactions between shards at each step.

import unittest
import pathlib
import random
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import key
import state_sync_lib
import transaction
import utils

EPOCH_LENGTH = 10

ONE_NEAR = 10**24
TGAS = 10**12

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

ALL_ACCOUNTS = [
    "aaa.test0",
    "ggg.test0",
    "lll.test0",
    "rrr.test0",
    "vvv.test0",
]

TxHash = str
AccountId = str


def random_target_height_generator(num_epochs):
    """Generates a random target height after num_epochs later."""
    target_height = 0
    while True:
        stop_height = random.randint(1, EPOCH_LENGTH)
        target_height += num_epochs * EPOCH_LENGTH + stop_height
        yield (target_height)


def random_u64():
    return bytes(random.randint(0, 255) for _ in range(8))


class MemtrieDiskTrieSwitchTest(unittest.TestCase):

    def setUp(self):
        self.nonces = {}
        self.keys = []
        self.txs = []

    def test(self):

        node_config_dump, node_config_sync = state_sync_lib.get_state_sync_configs_pair(
        )

        # Validator node configs.
        # Enable single-shard tracking with memtries in dumper node.
        node_config_sync["tracked_shards"] = [0]
        node_config_sync["store.load_mem_tries_for_tracked_shards"] = True
        configs = {x: node_config_sync for x in range(4)}

        # Dumper node config.
        # Enable all-shards tracking with memtries in dumper node.
        node_config_dump["tracked_shards"] = [0]
        node_config_dump["store.load_mem_tries_for_tracked_shards"] = True
        configs[4] = node_config_dump

        # RPC node config.
        node_config_sync["tracked_shards"] = [0]
        node_config_sync["store.load_mem_tries_for_tracked_shards"] = True
        configs[5] = node_config_sync

        self.nodes = cluster.start_cluster(
            num_nodes=4,
            num_observers=2,
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[
                ["epoch_length", EPOCH_LENGTH], ["shard_layout", SHARD_LAYOUT],
                ["shuffle_shard_assignment_for_chunk_producers", True],
                ["block_producer_kickout_threshold", 20],
                ["chunk_producer_kickout_threshold", 20]
            ],
            client_config_changes=configs)
        self.assertEqual(6, len(self.nodes))

        self.rpc_node = self.nodes[0]

        self.__create_accounts()

        self.__deploy_contracts()

        target_height_gen = random_target_height_generator(num_epochs=1)

        logger.info("Step 1: Running with memtries enabled")
        self.__random_workload_until(next(target_height_gen))
        self.__check_txs()

        logger.info("Step 2: Restarting nodes with memtries disabled")
        self.__restart_nodes(enable_memtries=False)
        self.__random_workload_until(next(target_height_gen))
        self.__check_txs()

        logger.info("Step 3: Restarting nodes with memtries enabled")
        self.__restart_nodes(enable_memtries=True)
        self.__random_workload_until(next(target_height_gen))
        self.__check_txs()

        logger.info("Test ended")

    def next_nonce(self, signer_key):
        assert signer_key in self.nonces
        nonce = self.nonces[signer_key]
        self.nonces[signer_key] = nonce + 1
        return nonce

    def __update_nonces(self, height):
        self.nonces = {
            account_key: nonce + 1_000_000 * height
            for account_key, nonce in self.nonces.items()
        }

    def __restart_nodes(self, enable_memtries):
        """Stops and restarts the nodes with the config that enables/disables memtries."""
        boot_node = self.nodes[0]
        for i in range(len(self.nodes)):
            self.nodes[i].kill()
            time.sleep(2)
            self.nodes[i].change_config(
                {"store.load_mem_tries_for_tracked_shards": enable_memtries})
            self.nodes[i].start(boot_node=None if i == 0 else boot_node)

    def __random_workload_until(self, target_height):
        """Generates traffic to make transfers between accounts."""
        logger.info(f"Running workload until height {target_height}")
        last_height = -1
        while True:
            last_block = self.rpc_node.get_latest_block()
            height = last_block.height
            if height > target_height:
                break
            if height != last_height:
                logger.info(
                    f'@{height}, epoch_height: {state_sync_lib.approximate_epoch_height(height, EPOCH_LENGTH)}'
                )
                last_height = height

            last_block_hash = self.rpc_node.get_latest_block().hash_bytes
            if random.random() < 0.5:
                # Make a transfer between accounts.
                # The goal is to generate cross-shard receipts.
                from_account_key = random.choice(self.account_keys)
                to_account_id = random.choice([
                    account_key.account_id
                    for account_key in self.account_keys
                    if account_key.account_id != from_account_key.account_id
                ] + ["near"])
                payment_tx = transaction.sign_payment_tx(
                    from_account_key, to_account_id, 1,
                    self.next_nonce(from_account_key), last_block_hash)
                result = self.rpc_node.send_tx(payment_tx)
                assert 'result' in result and 'error' not in result, (
                    'Expected "result" and no "error" in response, got: {}'.
                    format(result))
                logger.info("Transfer: {}".format(result))
                tx_hash = result['result']
                self.txs.append((from_account_key.account_id, tx_hash))
            elif len(self.keys) > 10 and random.random() < 0.5:
                # Do some flat storage reads, but only if we have enough keys populated.
                key = self.keys[random.randint(0, len(self.keys) - 1)]
                for account_key in self.account_keys:
                    tx = transaction.sign_function_call_tx(
                        account_key, account_key.account_id,
                        'read_value', key, 300 * TGAS, 0,
                        self.next_nonce(account_key), last_block_hash)
                    result = self.rpc_node.send_tx(tx)
                    assert 'result' in result and 'error' not in result, (
                        'Expected "result" and no "error" in response, got: {}'.
                        format(result))
                    logger.debug("Read value: {}".format(result))
                    tx_hash = result['result']
                    self.txs.append((account_key.account_id, tx_hash))
            else:
                # Generate some data for flat storage reads
                key = random_u64()
                self.keys.append(key)
                for account_key in self.account_keys:
                    tx = transaction.sign_function_call_tx(
                        account_key, account_key.account_id, 'write_key_value',
                        key + random_u64(), 300 * TGAS, 0,
                        self.next_nonce(account_key), last_block_hash)
                    result = self.rpc_node.send_tx(tx)
                    assert 'result' in result and 'error' not in result, (
                        'Expected "result" and no "error" in response, got: {}'.
                        format(result))
                    logger.debug("Wrote value: {}".format(result))
                    tx_hash = result['result']
                    self.txs.append((account_key.account_id, tx_hash))
            time.sleep(0.5)

    def __deploy_contracts(self):
        deploy_contract_tx_list = []
        for account_key in self.account_keys:
            contract = utils.load_test_contract()
            last_block_hash = self.rpc_node.get_latest_block().hash_bytes
            deploy_contract_tx = transaction.sign_deploy_contract_tx(
                account_key, contract, self.next_nonce(account_key),
                last_block_hash)
            result = self.rpc_node.send_tx(deploy_contract_tx)
            assert 'result' in result and 'error' not in result, (
                'Expected "result" and no "error" in response, got: {}'.format(
                    result))
            tx_hash = result['result']
            deploy_contract_tx_list.append((account_key.account_id, tx_hash))
            logger.info(
                f"Deploying contract for account: {account_key.account_id}, tx: {tx_hash}"
            )
        self.__wait_for_txs(deploy_contract_tx_list)

    def __create_accounts(self):
        account_keys = []
        for account_id in ALL_ACCOUNTS:
            account_key = key.Key.from_random(account_id)
            account_keys.append(account_key)

        signer_key = self.nodes[0].signer_key
        signer_nonce = self.rpc_node.get_nonce_for_pk(signer_key.account_id,
                                                      signer_key.pk) + 42

        create_account_tx_list = []
        for account_key in account_keys:
            tx_hash = self.__create_account(account_key, 1000 * ONE_NEAR,
                                            signer_key, signer_nonce)
            signer_nonce += 1
            create_account_tx_list.append((signer_key.account_id, tx_hash))
            logger.info(
                f"Creating account: {account_key.account_id}, tx: {tx_hash}")
        self.__wait_for_txs(create_account_tx_list)

        for account_key in account_keys:
            nonce = self.rpc_node.get_nonce_for_pk(account_key.account_id,
                                                   account_key.pk) + 42
            self.nonces[account_key] = nonce

        self.account_keys = account_keys

    def __create_account(self, account_key, balance, signer_key, signer_nonce):
        block_hash = self.rpc_node.get_latest_block().hash_bytes
        new_signer_key = key.Key(
            account_key.account_id,
            account_key.pk,
            account_key.sk,
        )
        create_account_tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
            signer_key,
            account_key.account_id,
            new_signer_key,
            balance,
            signer_nonce,
            block_hash,
        )
        result = self.rpc_node.send_tx(create_account_tx)
        self.assertIn('result', result, result)
        tx_hash = result['result']
        return tx_hash

    def __wait_for_txs(self, tx_list: list[AccountId, TxHash]):
        (height, _) = utils.wait_for_blocks(self.rpc_node, count=3)

        self.__update_nonces(height)

        for (tx_sender, tx_hash) in tx_list:
            self.__get_tx_status(tx_hash, tx_sender)

    def __check_txs(self):
        logger.info(f"Checking {len(self.txs)} transactions")
        self.__wait_for_txs(self.txs)
        self.txs = []

    def __get_tx_status(self, tx_hash, tx_sender):
        result = self.rpc_node.get_tx(tx_hash, tx_sender, timeout=15)
        self.assertIn('result', result, result)

        status = result['result']['final_execution_status']
        self.assertIn(status, GOOD_FINAL_EXECUTION_STATUS, result)

        status = result['result']['status']
        self.assertIn('SuccessValue', status, result)


if __name__ == '__main__':
    unittest.main()
