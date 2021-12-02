#!/usr/bin/env python3

import dataclasses
import json
import os
import pathlib
import sys
import time
import typing
import unittest

import base58
import ed25519
import requests
import requests.exceptions

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import key

_Dict = typing.Dict[str, typing.Any]
BlockIdentifier = typing.Union[str, int, _Dict]
TransIdentifier = typing.Union[str, _Dict]


def block_identifier(block_id: BlockIdentifier) -> _Dict:
    if isinstance(block_id, int):
        return {'index': block_id}
    if isinstance(block_id, str):
        return {'hash': block_id}
    if isinstance(block_id, dict):
        return block_id
    raise TypeError(f'{type(block_id).__name__} is not a block identifier')


def trans_identifier(trans_id: TransIdentifier) -> _Dict:
    if isinstance(trans_id, str):
        return {'hash': trans_id}
    if isinstance(trans_id, dict):
        return trans_id
    raise TypeError(
        f'{type(trans_id).__name__} is not a transaction identifier')


class RosettaRPC:
    href: str
    network_identifier: _Dict

    def __init__(self, *, host: str = '127.0.0.1', port: int = 5040) -> None:
        self.href = f'http://{host}:{port}'
        self.network_identifier = self.get_network_identifier()

    def get_network_identifier(self):
        result = requests.post(f'{self.href}/network/list',
                               headers={'content-type': 'application/json'},
                               data=json.dumps({'metadata': {}}))
        result.raise_for_status()
        return result.json()['network_identifiers'][0]

    def rpc(self, path: str, **data: typing.Any) -> _Dict:
        data['network_identifier'] = self.network_identifier
        result = requests.post(f'{self.href}{path}',
                               headers={'content-type': 'application/json'},
                               data=json.dumps(data, indent=True))
        result.raise_for_status()
        data = result.json()
        if 'code' in result:
            raise RuntimeError(f'Got error from {path}:\n{json.dumps(data)}')
        return data

    def exec_operations(self, signer: key.Key, *operations) -> str:
        public_key = {
            'hex_bytes': signer.decoded_pk().hex(),
            'curve_type': 'edwards25519'
        }
        options = self.rpc('/construction/preprocess',
                           operations=operations)['options']
        metadata = self.rpc('/construction/metadata',
                            options=options,
                            public_keys=[public_key])['metadata']
        payloads = self.rpc('/construction/payloads',
                            operations=operations,
                            public_keys=[public_key],
                            metadata=metadata)
        payload = payloads['payloads'][0]
        unsigned = payloads['unsigned_transaction']
        signature = signer.sign_bytes(bytearray.fromhex(payload['hex_bytes']))
        signed = self.rpc('/construction/combine',
                          unsigned_transaction=unsigned,
                          signatures=[{
                              'signing_payload': payload,
                              'hex_bytes': signature.hex(),
                              'signature_type': 'ed25519',
                              'public_key': public_key
                          }])['signed_transaction']
        tx = self.rpc('/construction/submit', signed_transaction=signed)
        return tx['transaction_identifier']['hash']

    def transfer(self, *, src: key.Key, dst: key.Key, amount: int) -> str:
        currency = {'symbol': 'NEAR', 'decimals': 24}
        return self.exec_operations(
            src, {
                'operation_identifier': {
                    'index': 0
                },
                'type': 'TRANSFER',
                'account': {
                    'address': src.account_id
                },
                'amount': {
                    'value': str(-amount),
                    'currency': currency
                },
            }, {
                'operation_identifier': {
                    'index': 1
                },
                'related_operations': [{
                    'index': 0
                }],
                'type': 'TRANSFER',
                'account': {
                    'address': dst.account_id
                },
                'amount': {
                    'value': str(amount),
                    'currency': currency
                },
            })

    def delete_account(self, account: key.Key, refund_to: key.Key) -> str:
        return self.exec_operations(
            account,
            {
                'operation_identifier': {
                    'index': 0
                },
                'type': 'INITIATE_DELETE_ACCOUNT',
                'account': {
                    'address': account.account_id
                },
            },
            {
                'operation_identifier': {
                    'index': 0
                },
                'type': 'DELETE_ACCOUNT',
                'account': {
                    'address': account.account_id
                },
            },
            {
                'operation_identifier': {
                    'index': 0
                },
                'type': 'REFUND_DELETE_ACCOUNT',
                'account': {
                    'address': refund_to.account_id
                },
            },
        )

    def get_block(self, *, block_id: BlockIdentifier) -> _Dict:
        return self.rpc('/block', block_identifier=block_identifier(block_id))

    def get_transaction(self, *, block_id: BlockIdentifier,
                        trans_id: TransIdentifier) -> _Dict:
        return self.rpc('/block/transaction',
                        block_identifier=block_identifier(block_id),
                        transaction_identifier=trans_identifier(trans_id))


class RosettaTestCase(unittest.TestCase):
    node = None
    rosetta = None

    def __init__(self, *args, **kw) -> None:
        super().__init__(*args, **kw)
        self.maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.node = cluster.start_cluster(1, 0, 1, {}, {}, {
            0: {
                'rosetta_rpc': {
                    'addr': '0.0.0.0:5040',
                    'cors_allowed_origins': ['*']
                },
            }
        })[0]
        cls.rosetta = RosettaRPC(host=cls.node.rpc_addr()[0])

    @classmethod
    def tearDownClass(cls) -> None:
        cls.node.cleanup()

    def test_get_block(self) -> None:
        """Tests getting blocks and transactions.

        Fetches the first and second blocks to see if the responses look as they
        should.  Then fetches one transaction from each of those blocks to again
        see if the returned data looks as expected.  Since the exact hashes
        differ each time the test runs, those are assumed to be correct.
        """
        block_0 = self.rosetta.get_block(block_id=0)
        block_0_id = block_0['block']['block_identifier']
        trans_0_id = 'block:' + block_0_id['hash']
        trans_0 = {
            'metadata': {
                'type': 'BLOCK'
            },
            'operations': [],
            'transaction_identifier': {
                'hash': trans_0_id
            }
        }
        self.assertEqual(
            {
                'block': {
                    'block_identifier': block_0_id,
                    # Genesis block’s parent is genesis block itself.
                    'parent_block_identifier': block_0_id,
                    'timestamp': block_0['block']['timestamp'],
                    'transactions': [trans_0]
                }
            },
            block_0)

        # Getting by hash should work and should return the exact same thing
        self.assertEqual(block_0,
                         self.rosetta.get_block(block_id=block_0_id['hash']))

        # Get transaction from genesis block.
        self.assertEqual({'transaction': trans_0},
                         self.rosetta.get_transaction(block_id=block_0_id,
                                                      trans_id=trans_0_id))

        # Block at height=1 should have genesis block as parent and only
        # validator update as a single operation.
        block_1 = self.rosetta.get_block(block_id=1)
        block_1_id = block_1['block']['block_identifier']
        trans_1_id = {'hash': 'block-validators-update:' + block_1_id['hash']}
        trans_1 = {
            'metadata': {
                'type': 'TRANSACTION'
            },
            'operations': [],
            'transaction_identifier': trans_1_id,
        }
        self.assertEqual(
            {
                'block': {
                    'block_identifier': {
                        'hash': block_1_id['hash'],
                        'index': 1
                    },
                    'parent_block_identifier':
                        block_0_id,
                    'timestamp':
                        block_1['block']['timestamp'],
                    'transactions': [{
                        'metadata': {
                            'type': 'TRANSACTION'
                        },
                        'operations': [],
                        'transaction_identifier': trans_1_id
                    }]
                }
            }, block_1)

        # Get transaction from the second block
        self.assertEqual({'transaction': trans_1},
                         self.rosetta.get_transaction(block_id=block_1_id,
                                                      trans_id=trans_1_id))

    def test_get_block_nonexistent(self) -> None:
        """Tests querying non-existent blocks and transactions.

        Queries for various blocks and transactions which do not exist on the
        chain to see if responses are what they should be.
        """
        block_0 = self.rosetta.get_block(block_id=0)
        block_0_id = block_0['block']['block_identifier']
        trans_0_id = 'block:' + block_0_id['hash']

        block_1 = self.rosetta.get_block(block_id=1)
        block_1_id = block_1['block']['block_identifier']
        trans_1_id = 'block:' + block_1_id['hash']

        def test(want_code, callback, *args, **kw) -> _Dict:
            with self.assertRaises(requests.exceptions.HTTPError) as err:
                callback(*args, **kw)
            self.assertEqual(500, err.exception.response.status_code)
            resp = err.exception.response.json()
            self.assertFalse(resp['retriable'])
            self.assertEqual(want_code, resp['code'])
            return resp

        # Query for non-existent blocks
        bogus_block_hash = 'GJ92SsB76CvfaHHdaC4Vsio6xSHT7fR3EEUoK84tFe99'
        self.assertEqual({'block': None},
                         self.rosetta.get_block(block_id=bogus_block_hash))

        test(400, self.rosetta.get_block, block_id='malformed-hash')

        # Query for non-existent transactions
        test(404,
             self.rosetta.get_transaction,
             block_id=block_0_id,
             trans_id=trans_1_id)
        test(404,
             self.rosetta.get_transaction,
             block_id=block_1_id,
             trans_id=trans_0_id)

    def test_delete_implicit_account(self) -> None:
        """Tests creating and deleting implicit account

        First sends some funds from validator’s account to an implicit account
        and the deletes that account refunding the validator account.
        """
        validator = self.node.validator_key
        implicit = key.Key.implicit_account()

        logger.info(f'Creating implicit account: {implicit.account_id}')
        tx_hash = self.rosetta.transfer(src=validator,
                                        dst=implicit,
                                        amount=10**22)
        logger.info(f'Transaction: {tx_hash}')

        for _ in range(10):
            time.sleep(1)
            result = self.node.get_account(implicit.account_id)
            if 'error' not in result:
                result = result['result']
                amount = result['amount']
                logger.info(f'Account balance {amount}')
                self.assertEqual(10**22, int(amount), result)
                break
        else:
            self.fail(f'Account {implicit.account_id} wasn’t created:\n'
                      f'{result}')

        logger.info(f'Deleting implicit account: {implicit.account_id}')
        tx_hash = self.rosetta.delete_account(implicit, refund_to=validator)
        logger.info(f'Transaction: {tx_hash}')

        for _ in range(10):
            time.sleep(1)
            result = self.node.get_account(implicit.account_id)
            if ('error' in result and
                    result['error']['cause']['name'] == 'UNKNOWN_ACCOUNT'):
                break
        else:
            self.fail(f'Account {implicit.account_id} wasn’t deleted:\n'
                      f'{result}')


if __name__ == '__main__':
    unittest.main()
