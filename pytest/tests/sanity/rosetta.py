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

JsonDict = typing.Dict[str, typing.Any]
BlockIdentifier = typing.Union[str, int, JsonDict]
TxIdentifier = typing.Union[str, JsonDict]


def block_identifier(block_id: BlockIdentifier) -> JsonDict:
    if isinstance(block_id, int):
        return {'index': block_id}
    if isinstance(block_id, str):
        return {'hash': block_id}
    if isinstance(block_id, dict):
        return block_id
    raise TypeError(f'{type(block_id).__name__} is not a block identifier')


def tx_identifier(tx_id: TxIdentifier) -> JsonDict:
    if isinstance(tx_id, str):
        return {'hash': tx_id}
    if isinstance(tx_id, dict):
        return tx_id
    raise TypeError(f'{type(tx_id).__name__} is not a transaction identifier')


class RosettaExecResult:
    identifier: typing.Dict[str, str]
    _rpc: 'RosettaRPC'
    _block_id: BlockIdentifier
    __block: typing.Optional[JsonDict] = None
    __transaction: typing.Optional[JsonDict] = None

    def __init__(self, rpc: 'RosettaRPC', block_id: BlockIdentifier,
                 identifier: typing.Dict[str, str]) -> None:
        self._rpc = rpc
        self._block_id = block_id
        self.identifier = identifier

    @property
    def hash(self) -> str:
        """The Rosetta hash of the transaction identifier.

        This will be a value in ‘<prefix>:<base58-hash>’ format as used in
        Rosetta RPC where prefix is either ‘tx’ for transactions or ‘receipt’
        for receipts.
        """
        return self.identifier['hash']

    @property
    def near_hash(self) -> str:
        """A NEAR transaction hash in base85.

        Compared to `hash` it’s just the `<base58-hash>’ part of the Rosetta
        identifier which is the NEAR transaction or receipt hash (depending on
        what comes before the colon).  This can be used to query NEAR through
        JSON RPC.
        """
        return self.identifier['hash'].split(':')[1]

    def block(self) -> JsonDict:
        """Returns the block in which the transaction was executed.

        When this method or `transaction` method is called the first time, it
        queries the node to find the node which includes the transaction.  The
        return value is memoised so subsequent calls won’t do the querying.

        Returns:
            A Rosetta RPC block data object.
        """
        return self.__get_transaction()[0]

    def transaction(self) -> JsonDict:
        """Returns the transaction details from Rosetta RPC.

        When this method or `block` method is called the first time, it queries
        the node to find the node which includes the transaction.  The return
        value is memoised so subsequent calls won’t do the querying.

        Returns:
            A Rosetta RPC transaction data object.
        """
        return self.__get_transaction()[1]

    def related(self, num: int) -> typing.Optional[JsonDict]:
        """Returns related transaction or None if there aren’t that many.

        The method uses `transaction` method so all comments regarding fetching
        the data from node apply to it as well.

        Returns:
            If the transaction has at least `num+1` related transactions returns
            a new `RosettaExecResult` object which can be used to fetch the
            transaction.  Otherwise, returns None.
        """
        block, transaction = self.__get_transaction()
        related = transaction.get('related_transactions', ())
        if len(related) <= num:
            return None
        return type(self)(self._rpc, block['block_identifier'],
                          related[num]['transaction_identifier'])

    def __get_transaction(self) -> typing.Tuple[JsonDict, JsonDict]:
        """Fetches transaction and its block from the node if not yet retrieved.

        Returns:
            (block, transaction) tuple where first element is Rosetta Block
            object and second Rosetta Transaction object.
        """
        if self.__block and self.__transaction:
            return self.__block, self.__transaction
        timeout = time.monotonic() + 10
        while time.monotonic() < timeout:
            while True:
                try:
                    block = self._rpc.get_block(block_id=self._block_id)
                except RuntimeError:
                    block = None
                if not block:
                    break
                for tx in block['transactions']:
                    if tx['transaction_identifier']['hash'] == self.hash:
                        related = ', '.join(
                            related['transaction_identifier']['hash']
                            for related in tx.get('related_transactions',
                                                  ())) or 'none'
                        logger.info(f'Receipts of {self.hash}: {related}')
                        self.__memoised = (block, tx)
                        return self.__memoised
                self._block_id = int(block['block_identifier']['index']) + 1
            time.sleep(0.25)
        assert False, f'Transaction {self.hash} did not complete in 10 seconds'


class RosettaRPC:
    node: cluster.BaseNode
    href: str
    network_identifier: JsonDict

    def __init__(self,
                 *,
                 node: cluster.BaseNode,
                 host: str = '127.0.0.1',
                 port: int = 5040) -> None:
        self.node = node
        self.href = f'http://{host}:{port}'
        self.network_identifier = self.get_network_identifier()

    def get_network_identifier(self):
        result = requests.post(f'{self.href}/network/list',
                               headers={'content-type': 'application/json'},
                               data=json.dumps({'metadata': {}}))
        result.raise_for_status()
        return result.json()['network_identifiers'][0]

    def rpc(self, path: str, **data: typing.Any) -> JsonDict:
        data['network_identifier'] = self.network_identifier
        result = requests.post(f'{self.href}{path}',
                               headers={'content-type': 'application/json'},
                               data=json.dumps(data, indent=True))
        result.raise_for_status()
        data = result.json()
        if 'code' in result:
            raise RuntimeError(f'Got error from {path}:\n{json.dumps(data)}')
        return data

    def _get_latest_block_height(self) -> int:
        """Returns latest block’s height."""
        block_hash = self.node.get_status()['sync_info']['latest_block_hash']
        return self.node.get_block(block_hash)['result']['header']['height']

    def exec_operations(self, signer: key.Key,
                        *operations) -> RosettaExecResult:
        """Sends given operations to Construction API.

        Args:
            signer: Account signing the operations.
            operations: List of operations to perform.
        Returns:
            A RosettaExecResult object which can be used to get hash of the
            submitted transaction or wait on the transaction completion.
        """
        height = self._get_latest_block_height()

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
        tx_hash = tx['transaction_identifier']['hash']
        logger.info(f'Transaction hash: {tx_hash}')
        return RosettaExecResult(self, height, tx['transaction_identifier'])

    def transfer(self, *, src: key.Key, dst: key.Key, amount: int,
                 **kw) -> RosettaExecResult:
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
            }, **kw)

    def delete_account(self, account: key.Key, refund_to: key.Key,
                       **kw) -> RosettaExecResult:
        return self.exec_operations(
            account, {
                'operation_identifier': {
                    'index': 0
                },
                'type': 'INITIATE_DELETE_ACCOUNT',
                'account': {
                    'address': account.account_id
                },
            }, {
                'operation_identifier': {
                    'index': 0
                },
                'type': 'DELETE_ACCOUNT',
                'account': {
                    'address': account.account_id
                },
            }, {
                'operation_identifier': {
                    'index': 0
                },
                'type': 'REFUND_DELETE_ACCOUNT',
                'account': {
                    'address': refund_to.account_id
                },
            }, **kw)

    def get_block(self, *, block_id: BlockIdentifier) -> JsonDict:
        res = self.rpc('/block', block_identifier=block_identifier(block_id))
        return res['block']

    def get_transaction(self, *, block_id: BlockIdentifier,
                        tx_id: TxIdentifier) -> JsonDict:
        res = self.rpc('/block/transaction',
                       block_identifier=block_identifier(block_id),
                       transaction_identifier=tx_identifier(tx_id))
        return res['transaction']


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
        cls.rosetta = RosettaRPC(node=cls.node, host=cls.node.rpc_addr()[0])

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

        def normalise_operations(transactions: typing.Sequence[typing.Any]):
            """Normalises operations in a transactions by sorting by value."""
            for tr in transactions:
                ops = tr.get('operations', [])
                ops.sort(key=lambda op: int(op['amount']['value']))
                for idx, op in enumerate(ops):
                    op['operation_identifier']['index'] = idx

        block_0 = self.rosetta.get_block(block_id=0)
        normalise_operations(block_0['transactions'])
        block_0_id = block_0['block_identifier']
        trans_0_id = 'block:' + block_0_id['hash']
        trans_0 = {
            'metadata': {
                'type': 'BLOCK'
            },
            'operations': [{
                'account': {
                    'address': 'near',
                    'sub_account': {
                        'address': 'LIQUID_BALANCE_FOR_STORAGE'
                    }
                },
                'amount': {
                    'currency': {
                        'decimals': 24,
                        'symbol': 'NEAR'
                    },
                    'value': '1820000000000000000000'
                },
                'operation_identifier': {
                    'index': 0
                },
                'status': 'SUCCESS',
                'type': 'TRANSFER'
            }, {
                'account': {
                    'address': 'test0',
                    'sub_account': {
                        'address': 'LOCKED'
                    }
                },
                'amount': {
                    'currency': {
                        'decimals': 24,
                        'symbol': 'NEAR'
                    },
                    'value': '50000000000000000000000000000000'
                },
                'operation_identifier': {
                    'index': 1
                },
                'status': 'SUCCESS',
                'type': 'TRANSFER'
            }, {
                'account': {
                    'address': 'test0'
                },
                'amount': {
                    'currency': {
                        'decimals': 24,
                        'symbol': 'NEAR'
                    },
                    'value': '950000000000000000000000000000000'
                },
                'operation_identifier': {
                    'index': 2
                },
                'status': 'SUCCESS',
                'type': 'TRANSFER'
            }, {
                'account': {
                    'address': 'near'
                },
                'amount': {
                    'currency': {
                        'decimals': 24,
                        'symbol': 'NEAR'
                    },
                    'value': '999999999998180000000000000000000'
                },
                'operation_identifier': {
                    'index': 3
                },
                'status': 'SUCCESS',
                'type': 'TRANSFER'
            }],
            'transaction_identifier': {
                'hash': trans_0_id
            }
        }
        self.assertEqual(
            {
                'block_identifier': block_0_id,
                # Genesis block’s parent is genesis block itself.
                'parent_block_identifier': block_0_id,
                'timestamp': block_0['timestamp'],
                'transactions': [trans_0]
            },
            block_0)

        # Getting by hash should work and should return the exact same thing
        block = self.rosetta.get_block(block_id=block_0_id['hash'])
        normalise_operations(block['transactions'])
        self.assertEqual(block_0, block)

        # Get transaction from genesis block.
        tr = self.rosetta.get_transaction(block_id=block_0_id, tx_id=trans_0_id)
        normalise_operations((tr,))
        self.assertEqual(trans_0, tr)

        # Block at height=1 should have genesis block as parent and only
        # validator update as a single operation.
        block_1 = self.rosetta.get_block(block_id=1)
        block_1_id = block_1['block_identifier']
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
                'block_identifier': {
                    'hash': block_1_id['hash'],
                    'index': 1
                },
                'parent_block_identifier':
                    block_0_id,
                'timestamp':
                    block_1['timestamp'],
                'transactions': [{
                    'metadata': {
                        'type': 'TRANSACTION'
                    },
                    'operations': [],
                    'transaction_identifier': trans_1_id
                }]
            }, block_1)

        # Get transaction from the second block
        self.assertEqual(
            trans_1,
            self.rosetta.get_transaction(block_id=block_1_id, tx_id=trans_1_id))

    def test_get_block_nonexistent(self) -> None:
        """Tests querying non-existent blocks and transactions.

        Queries for various blocks and transactions which do not exist on the
        chain to see if responses are what they should be.
        """
        block_0 = self.rosetta.get_block(block_id=0)
        block_0_id = block_0['block_identifier']
        trans_0_id = 'block:' + block_0_id['hash']

        block_1 = self.rosetta.get_block(block_id=1)
        block_1_id = block_1['block_identifier']
        trans_1_id = 'block:' + block_1_id['hash']

        def test(want_code, callback, *args, **kw) -> JsonDict:
            with self.assertRaises(requests.exceptions.HTTPError) as err:
                callback(*args, **kw)
            self.assertEqual(500, err.exception.response.status_code)
            resp = err.exception.response.json()
            self.assertFalse(resp['retriable'])
            self.assertEqual(want_code, resp['code'])
            return resp

        # Query for non-existent blocks
        bogus_block_hash = 'GJ92SsB76CvfaHHdaC4Vsio6xSHT7fR3EEUoK84tFe99'
        self.assertIsNone(self.rosetta.get_block(block_id=bogus_block_hash))

        test(400, self.rosetta.get_block, block_id='malformed-hash')

        # Query for non-existent transactions
        test(404,
             self.rosetta.get_transaction,
             block_id=block_0_id,
             tx_id=trans_1_id)
        test(404,
             self.rosetta.get_transaction,
             block_id=block_1_id,
             tx_id=trans_0_id)

    def _get_account_balance(self,
                             account: key.Key,
                             require: bool = True) -> typing.Optional[int]:
        """Returns balance of given account or None if account doesn’t exist.

        Args:
            account: Account to get balance of.
            require: If True, require that the account exists.
        """
        account_id = account.account_id
        result = self.node.get_account(account_id, do_assert=False)
        error = result.get('error')
        if error is None:
            amount = int(result['result']['amount'])
            logger.info(f'Account {account_id} balance: {amount} yocto')
            return amount
        self.assertEqual('UNKNOWN_ACCOUNT', error['cause']['name'],
                         f'Error fetching account {account_id}: {error}')
        if require:
            self.fail(f'Account {account.account_id} does not exist')
        return None

    def test_implicit_account(self) -> None:
        """Tests creating and deleting implicit account

        First sends some funds from validator’s account to an implicit account,
        then checks how the transaction looks through Data API and finally
        deletes that account refunding the validator account.
        """
        test_amount = 10**22
        validator = self.node.validator_key
        implicit = key.Key.implicit_account()

        ### 1. Create implicit account.
        logger.info(f'Creating implicit account: {implicit.account_id}')
        result = self.rosetta.transfer(src=validator,
                                       dst=implicit,
                                       amount=test_amount)
        # Get the transaction through Rosetta RPC.
        block = result.block()
        tx = result.transaction()
        # Also get it from JSON RPC to compare receipt ids.
        json_res = self.node.get_tx(result.near_hash, implicit.account_id)
        json_res = json_res['result']
        receipt_ids = json_res['transaction_outcome']['outcome']['receipt_ids']
        self.assertEqual(1, len(receipt_ids))
        receipt_id = {'hash': 'receipt:' + receipt_ids[0]}

        # The actual amount subtracted is more than test_amount because of the
        # gas payment.
        value = -int(tx['operations'][0]['amount']['value'])
        logger.info(f'Took {value} from validator account')
        self.assertLess(test_amount, value)

        self.assertEqual([{
            'metadata': {
                'type': 'TRANSACTION'
            },
            'operations': [{
                'account': {
                    'address': 'test0'
                },
                'amount': {
                    'currency': {
                        'decimals': 24,
                        'symbol': 'NEAR'
                    },
                    'value': str(-value)
                },
                'operation_identifier': {
                    'index': 0
                },
                'status': 'SUCCESS',
                'type': 'TRANSFER'
            }],
            'related_transactions': [{
                'direction': 'forward',
                'transaction_identifier': receipt_id,
            }],
            'transaction_identifier': result.identifier,
        }], block['transactions'])

        # Fetch the receipt through Rosetta RPC.
        result = RosettaExecResult(self.rosetta, block, receipt_id)
        related = result.related(0)
        self.assertEqual(
            {
                'transaction_identifier': result.identifier,
                'operations': [{
                    'operation_identifier': {
                        'index': 0
                    },
                    'type': 'TRANSFER',
                    'status': 'SUCCESS',
                    'account': {
                        'address': implicit.account_id,
                    },
                    'amount': {
                        'value': '8180000000000000000000',
                        'currency': {
                            'symbol': 'NEAR',
                            'decimals': 24
                        }
                    }
                }, {
                    'operation_identifier': {
                        'index': 1
                    },
                    'type': 'TRANSFER',
                    'status': 'SUCCESS',
                    'account': {
                        'address': implicit.account_id,
                        'sub_account': {
                            'address': 'LIQUID_BALANCE_FOR_STORAGE'
                        }
                    },
                    'amount': {
                        'value': '1820000000000000000000',
                        'currency': {
                            'symbol': 'NEAR',
                            'decimals': 24
                        }
                    }
                }],
                'related_transactions': [{
                    'direction': 'forward',
                    'transaction_identifier': related and related.identifier
                }],
                'metadata': {
                    'type': 'TRANSACTION'
                }
            }, result.transaction())

        # Fetch the next receipt through Rosetta RPC.
        self.assertEqual(
            {
                'metadata': {
                    'type': 'TRANSACTION'
                },
                'operations': [{
                    'account': {
                        'address': 'test0'
                    },
                    'amount': {
                        'currency': {
                            'decimals': 24,
                            'symbol': 'NEAR'
                        },
                        'value': '12736651875000000000'
                    },
                    'operation_identifier': {
                        'index': 0
                    },
                    'status': 'SUCCESS',
                    'type': 'TRANSFER'
                }],
                'transaction_identifier': related.identifier
            }, related.transaction())

        ### 2. Delete the account.
        logger.info(f'Deleting implicit account: {implicit.account_id}')
        result = self.rosetta.delete_account(implicit, refund_to=validator)

        self.assertEqual(
            test_amount, -sum(
                int(op['amount']['value'])
                for tx in result.block()['transactions']
                for op in tx['operations']))

        json_res = self.node.get_tx(result.near_hash, implicit.account_id)
        json_res = json_res['result']
        receipt_ids = json_res['transaction_outcome']['outcome']['receipt_ids']
        self.assertEqual(1, len(receipt_ids))
        receipt_id = {'hash': 'receipt:' + receipt_ids[0]}

        receipt_ids = json_res['receipts_outcome'][0]['outcome']['receipt_ids']
        self.assertEqual(1, len(receipt_ids))
        receipt_id_2 = {'hash': 'receipt:' + receipt_ids[0]}

        self.assertEqual(
            {
                'metadata': {
                    'type': 'TRANSACTION'
                },
                'operations': [{
                    'account': {
                        'address': implicit.account_id,
                    },
                    'amount': {
                        'currency': {
                            'decimals': 24,
                            'symbol': 'NEAR'
                        },
                        'value': '-511097000000000000000'
                    },
                    'operation_identifier': {
                        'index': 0
                    },
                    'status': 'SUCCESS',
                    'type': 'TRANSFER'
                }],
                'related_transactions': [{
                    'direction': 'forward',
                    'transaction_identifier': receipt_id,
                }],
                'transaction_identifier': result.identifier
            }, result.transaction())

        # Fetch the receipt
        result = RosettaExecResult(self.rosetta, block, receipt_id)
        self.assertEqual(
            {
                'metadata': {
                    'type': 'TRANSACTION'
                },
                'operations': [{
                    'account': {
                        'address': implicit.account_id,
                    },
                    'amount': {
                        'currency': {
                            'decimals': 24,
                            'symbol': 'NEAR'
                        },
                        'value': '-7668903000000000000000'
                    },
                    'operation_identifier': {
                        'index': 0
                    },
                    'status': 'SUCCESS',
                    'type': 'TRANSFER'
                }, {
                    'account': {
                        'address': implicit.account_id,
                        'sub_account': {
                            'address': 'LIQUID_BALANCE_FOR_STORAGE'
                        }
                    },
                    'amount': {
                        'currency': {
                            'decimals': 24,
                            'symbol': 'NEAR'
                        },
                        'value': '-1820000000000000000000'
                    },
                    'operation_identifier': {
                        'index': 1
                    },
                    'status': 'SUCCESS',
                    'type': 'TRANSFER'
                }],
                'related_transactions': [{
                    'direction': 'forward',
                    'transaction_identifier': receipt_id_2
                }],
                'transaction_identifier': receipt_id
            }, result.transaction())

        # Fetch receipt’s receipt
        result = RosettaExecResult(self.rosetta, block, receipt_id_2)
        self.assertEqual(
            {
                'metadata': {
                    'type': 'TRANSACTION'
                },
                'operations': [{
                    'account': {
                        'address': 'test0'
                    },
                    'amount': {
                        'currency': {
                            'decimals': 24,
                            'symbol': 'NEAR'
                        },
                        'value': '9488903000000000000000'
                    },
                    'operation_identifier': {
                        'index': 0
                    },
                    'status': 'SUCCESS',
                    'type': 'TRANSFER'
                }],
                'transaction_identifier': receipt_id_2
            }, result.transaction())


if __name__ == '__main__':
    unittest.main()
