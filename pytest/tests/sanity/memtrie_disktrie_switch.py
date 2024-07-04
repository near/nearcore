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

        # Validator node configs: Enable single-shard tracking with memtries enabled.
        node_config_sync["tracked_shards"] = []
        node_config_sync["store.load_mem_tries_for_tracked_shards"] = True
        configs = {x: node_config_sync for x in range(4)}

        # Dumper node config: Enable tracking all shards with memtries enabled.
        node_config_dump["tracked_shards"] = [0]
        node_config_dump["store.load_mem_tries_for_tracked_shards"] = True
        configs[4] = node_config_dump

        self.nodes = cluster.start_cluster(
            num_nodes=4,
            num_observers=1,
            num_shards=NUM_SHARDS,
            config=None,
            genesis_config_changes=[
                ["epoch_length", EPOCH_LENGTH], ["shard_layout", SHARD_LAYOUT],
                ["shuffle_shard_assignment_for_chunk_producers", True],
                ["block_producer_kickout_threshold", 0],
                ["chunk_producer_kickout_threshold", 0]
            ],
            client_config_changes=configs)
        self.assertEqual(5, len(self.nodes))

        # Use the dumper node as the RPC node for sending the transactions.
        self.rpc_node = self.nodes[4]

        self.__wait_for_blocks(3)

        self.__create_accounts()

        self.__deploy_contracts()

        target_height = self.__next_target_height(num_epochs=1)
        logger.info(
            f"Step 1: Running with memtries enabled until height {target_height}"
        )
        self.__random_workload_until(target_height)

        target_height = self.__next_target_height(num_epochs=1)
        logger.info(
            f"Step 2: Restarting nodes with memtries disabled until height {target_height}"
        )
        self.__restart_nodes(enable_memtries=False)
        self.__random_workload_until(target_height)

        target_height = self.__next_target_height(num_epochs=1)
        logger.info(f"Step 3: Restarting nodes with memtries enabled until height {target_height}")
        self.__restart_nodes(enable_memtries=True)
        self.__random_workload_until(target_height)

        self.__wait_for_txs(self.txs, assert_all_accepted=False)
        logger.info("Test ended")

    def __next_target_height(self, num_epochs):
        """Returns a next target height until which we will send the transactions."""
        current_height = self.__wait_for_blocks(1)
        stop_height = random.randint(1, EPOCH_LENGTH)
        return current_height + num_epochs * EPOCH_LENGTH + stop_height

    def next_nonce(self, signer_key):
        """Returns the next nonce to use for sending transactions for the given signing key."""
        assert signer_key in self.nonces
        nonce = self.nonces[signer_key]
        self.nonces[signer_key] = nonce + 42
        return nonce

    def __restart_nodes(self, enable_memtries):
        """Stops and restarts the nodes with the config that enables/disables memtries.
        
        It restarts only the validator nodes and does NOT restart the RPC node."""
        boot_node = self.rpc_node
        for i in range(0, 4):
            self.nodes[i].kill()
            time.sleep(2)
            self.nodes[i].change_config(
                {"store.load_mem_tries_for_tracked_shards": enable_memtries})
            self.nodes[i].start(boot_node=None if i == 0 else boot_node)

    def __random_workload_until(self, target_height):
        """Generates traffic to make transfers between accounts."""
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
            last_block_hash = last_block.hash_bytes
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
                logger.debug("Transfer: {}".format(result))
                tx_hash = result['result']
                self.txs.append((from_account_key.account_id, tx_hash))
            elif len(self.keys) > 10 and random.random() < 0.5:
                # Do some storage reads, but only if we have enough keys populated.
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
                # Generate some data for storage reads
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
        """Deploys test contract for each test account.
        
        Waits for the deploy-contract transactions to complete."""
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
        """Creates the test accounts.
        
        Waits for the create-account transactions to complete."""
        account_keys = []
        for account_id in ALL_ACCOUNTS:
            account_key = key.Key.from_random(account_id)
            account_keys.append(account_key)

        # Use the first validator node to sign the transactions.
        signer_key = self.nodes[0].signer_key
        # Update nonce of the signer account using the access key nonce.
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

        # Update nonces for the newly created accounts using the access key nonces.
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

    def __wait_for_txs(self,
                       tx_list: list[(AccountId, TxHash)],
                       assert_all_accepted=True):
        """Waits for the transactions to be accepted.
        
        If assert_all_accepted is True, it will assert that all transactions were accepted.
        Otherwise, it asserts that at least 1 of the transactions were accepted."""
        self.assertGreater(len(tx_list), 0)
        self.__wait_for_blocks(3)
        logger.info(f"Checking status of {len(tx_list)} transactions")
        accepted = 0
        rejected = 0
        for (tx_sender, tx_hash) in tx_list:
            if self.__get_tx_status(tx_hash, tx_sender):
                accepted += 1
                if not assert_all_accepted:
                    break
            else:
                rejected += 1
        if assert_all_accepted:
            self.assertEqual(accepted, len(tx_list))
        else:
            self.assertGreater(accepted, 0)

    def __get_tx_status(self, tx_hash, tx_sender) -> bool:
        """Checks the status of the transaction and returns true if it is accepted."""
        result = self.rpc_node.get_tx(tx_hash, tx_sender, timeout=10)
        if 'result' not in result:
            self.assertIn('error', result, result)
            return False

        status = result['result']['final_execution_status']
        self.assertIn(status, GOOD_FINAL_EXECUTION_STATUS, result)

        status = result['result']['status']
        self.assertIn('SuccessValue', status, result)

        return True

    def __wait_for_blocks(self, num_blocks):
        height, _ = utils.wait_for_blocks(self.rpc_node, count=num_blocks)
        return height


if __name__ == '__main__':
    unittest.main()
