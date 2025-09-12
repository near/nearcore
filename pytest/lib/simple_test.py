#!/usr/bin/env python3
"""Utilities for setting up a simple test involving a number of accounts and transactions that transfer tokens between them."""

import random
import time
from configured_logger import logger
import key
import state_sync_lib
import transaction
import utils

ONE_NEAR = 10**24
TGAS = 10**12

GOOD_FINAL_EXECUTION_STATUS = ['FINAL', 'EXECUTED', 'EXECUTED_OPTIMISTIC']

TxHash = str
AccountId = str


def random_u64():
    return bytes(random.randint(0, 255) for _ in range(8))


class SimpleTransferBetweenAccounts:
    """Simple test base to setup a test with 4 shards, a number of accounts, and transactions sending amount between them."""

    def __init__(self, nodes, rpc_node, account_prefixes, epoch_length):
        self.node_keys = [node.signer_key for node in nodes]
        self.rpc_node = rpc_node
        self.account_prefixes = account_prefixes
        self.epoch_length = epoch_length
        # To be initialized by create_accounts.
        self.account_keys = []
        # To be initialized by retrieve_nonces.
        self.nonces = {}
        # To be initialized by random_workload_until.
        self.keys = []
        self.txs = []

    def next_nonce(self, signer_key):
        """Returns the next nonce to use for sending transactions for the given signing key."""
        account_id = signer_key.account_id
        assert account_id in self.nonces
        nonce = self.nonces[account_id]
        self.nonces[account_id] = nonce + 1
        return nonce

    def retrieve_nonces(self, signer_keys):
        """Retrieves the next nonce for the given signer key and stores them locally."""
        for signer_key in signer_keys:
            assert signer_key.account_id not in self.nonces
            nonce = self.rpc_node.get_nonce_for_pk(signer_key.account_id,
                                                   signer_key.pk) + 1
            self.nonces[signer_key.account_id] = nonce

    def random_workload_until(self, target_height):
        """Generates traffic to make transfers between accounts."""
        logger.info(f"Running random workload until height={target_height}")
        last_height = -1
        while True:
            last_block = self.rpc_node.get_latest_block()
            height = last_block.height
            if height > target_height:
                break
            if height != last_height:
                logger.info(
                    f'@{height}, epoch_height: {state_sync_lib.approximate_epoch_height(height, self.epoch_length)}'
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

    def deploy_contracts(self):
        """Deploys test contract for each test account.

        Waits for the deploy-contract transactions to complete."""
        assert len(self.account_keys
                  ) > 0, "Accounts not initialized, call create_accounts first."
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
        self.wait_for_txs(assert_all_accepted=True,
                          tx_list=deploy_contract_tx_list)

    def create_accounts(self):
        """Creates the test accounts.

        Waits for the create-account transactions to complete."""

        # Update nonces for the validator accounts, as we will use them to create the new accounts.
        self.retrieve_nonces(self.node_keys)

        create_account_tx_list = []
        account_keys = []
        for i in range(len(self.account_prefixes)):
            # Choose one of the nodes to sign the transaction for creating the new account.
            # We do not use a single node to sign all the transactions, since transactions and
            # their nonces can be reordered, which would invalidate the transactions with smaller nonces.
            signer_key = self.node_keys[i % len(self.node_keys)]

            # Append the signer validator's account id to the account id to make is a valid AccountId.
            account_id = self.account_prefixes[i] + '.' + signer_key.account_id
            account_key = key.Key.from_random(account_id)
            account_keys.append(account_key)

            tx_hash = self.create_account(account_key, 1000 * ONE_NEAR,
                                          signer_key)
            create_account_tx_list.append((signer_key.account_id, tx_hash))
            logger.info(
                f"Creating account: {account_key.account_id}, tx: {tx_hash}")

        self.wait_for_txs(assert_all_accepted=True,
                          tx_list=create_account_tx_list)

        # Update nonces for the newly created accounts.
        self.retrieve_nonces(account_keys)

        self.account_keys = account_keys

    def create_account(self, account_key, balance, signer_key):
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
            self.next_nonce(signer_key),
            block_hash,
        )
        result = self.rpc_node.send_tx(create_account_tx)
        assert 'result' in result, result
        tx_hash = result['result']
        return tx_hash

    def wait_for_txs(self,
                     assert_all_accepted: bool,
                     tx_list: list[(AccountId, TxHash)] = None):
        """Waits for the transactions to be accepted.

        If assert_all_accepted is True, it will assert that all transactions were accepted.
        Otherwise, it asserts that at least 1 of the transactions were accepted."""
        if tx_list is None:
            tx_list = self.txs
        assert len(tx_list) > 0
        self.wait_for_blocks(3)
        logger.info(f"Checking status of {len(tx_list)} transactions")
        accepted = 0
        rejected = 0
        for (tx_sender, tx_hash) in tx_list:
            if self.get_tx_status(tx_hash, tx_sender):
                accepted += 1
                if not assert_all_accepted:
                    break
            else:
                rejected += 1
        if assert_all_accepted:
            assert accepted == len(tx_list)
        else:
            assert accepted > 0

    def get_tx_status(self, tx_hash, tx_sender) -> bool:
        """Checks the status of the transaction and returns true if it is accepted."""
        result = self.rpc_node.get_tx(tx_hash, tx_sender, timeout=10)
        if 'result' not in result:
            assert 'error' in result, result
            return False

        status = result['result']['final_execution_status']
        assert status in GOOD_FINAL_EXECUTION_STATUS, result

        status = result['result']['status']
        assert 'SuccessValue' in status, result

        return True

    def wait_for_blocks(self, num_blocks, timeout=None):
        height, _ = utils.wait_for_blocks(self.rpc_node,
                                          count=num_blocks,
                                          timeout=timeout)
        return height
