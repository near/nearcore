#!/usr/bin/env python3
# Tests the meta transaction flow.
# Creates a new account (candidate.test0) with a fixed amount of tokens.
# Afterwards, creates the meta transaction that adds a new key to this account, but the gas is paid by someone else (test0) account.
# At the end, verifies that key has been added succesfully and that the amount of tokens in candidate didn't change.

import base58
import pathlib
import sys
import typing

import unittest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, LocalNode
import utils
import transaction
import key


class Nonce:
    """ Helper class to manage nonces (automatically increase them when they are used. """

    def __init__(self, current_nonce: int = 0):
        self.nonce = current_nonce

    def use_nonce(self) -> int:
        self.nonce += 1
        return self.nonce


def create_nonce_from_node(node: LocalNode, account_id: str, pk: str) -> Nonce:
    nn = node.get_nonce_for_pk(account_id, pk)
    assert nn, "Nonce missing for the candidate account"
    return Nonce(nn)


# Returns the number of keys and current amount for a given account
def check_account_status(node: LocalNode,
                         account_id: str) -> typing.Tuple[int, int]:
    current_keys = node.get_access_key_list(account_id)['result']['keys']
    account_state = node.get_account(account_id)['result']
    return (len(current_keys), int(account_state['amount']))


class TestMetaTransactions(unittest.TestCase):

    def test_meta_tx(self):
        nodes: list[LocalNode] = start_cluster(2, 0, 1, None, [], {})
        _, hash_ = utils.wait_for_blocks(nodes[0], target=10)

        node0_nonce = Nonce()

        CANDIDATE_ACCOUNT = "candidate.test0"
        CANDIDATE_STARTING_AMOUNT = 123 * (10**24)

        # create new account
        candidate_key = key.Key.from_random(CANDIDATE_ACCOUNT)

        tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
            nodes[0].signer_key, candidate_key.account_id, candidate_key,
            CANDIDATE_STARTING_AMOUNT, node0_nonce.use_nonce(),
            base58.b58decode(hash_.encode('utf8')))
        nodes[0].send_tx_and_wait(tx, 100)

        self.assertEqual(check_account_status(nodes[0], CANDIDATE_ACCOUNT),
                         (1, CANDIDATE_STARTING_AMOUNT))

        candidate_nonce = create_nonce_from_node(nodes[0],
                                                 candidate_key.account_id,
                                                 candidate_key.pk)

        # Now let's prepare the meta transaction.
        new_key = key.Key.from_random("new_key")
        add_new_key_action = transaction.create_full_access_key_action(
            new_key.decoded_pk())
        signed_meta_tx = transaction.create_signed_delegated_action(
            CANDIDATE_ACCOUNT, CANDIDATE_ACCOUNT, [add_new_key_action],
            candidate_nonce.use_nonce(), 1000, candidate_key.decoded_pk(),
            candidate_key.decoded_sk())

        meta_tx = transaction.sign_delegate_action(
            signed_meta_tx, nodes[0].signer_key, CANDIDATE_ACCOUNT,
            node0_nonce.use_nonce(), base58.b58decode(hash_.encode('utf8')))

        nodes[0].send_tx_and_wait(meta_tx, 100)

        self.assertEqual(check_account_status(nodes[0], CANDIDATE_ACCOUNT),
                         (2, CANDIDATE_STARTING_AMOUNT))


if __name__ == '__main__':
    unittest.main()
