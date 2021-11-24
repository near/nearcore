import base58
import json
import os
import pathlib
import sys
import time
import typing
import unittest

import ed25519
import requests

sys.path.append('lib')

from configured_logger import logger
import cluster
import key

_Dict = typing.Dict[str, typing.Any]


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


def test_delete_implicit_account() -> None:
    node = cluster.start_cluster(1, 0, 1, {}, {}, {
        0: {
            'rosetta_rpc': {
                'addr': '0.0.0.0:5040',
                'cors_allowed_origins': ['*']
            },
        }
    })[0]
    rosetta = RosettaRPC(host=node.rpc_addr()[0])
    validator = node.validator_key
    implicit = key.Key.implicit_account()

    logger.info(f'Creating implicit account: {implicit.account_id}')
    tx_hash = rosetta.transfer(src=validator, dst=implicit, amount=10**22)
    logger.info(f'Transaction: {tx_hash}')

    for _ in range(10):
        time.sleep(1)
        result = node.get_account(implicit.account_id)
        if 'error' not in result:
            result = result['result']
            amount = result['amount']
            logger.info(f'Account balance {amount}')
            assert int(amount) == 10**22, result
            break
    else:
        assert False, f'Account {implicit.account_id} wasn’t created:\n{result}'

    logger.info(f'Deleting implicit account: {implicit.account_id}')
    tx_hash = rosetta.delete_account(implicit, refund_to=validator)
    logger.info(f'Transaction: {tx_hash}')

    for _ in range(10):
        time.sleep(1)
        result = node.get_account(implicit.account_id)
        if ('error' in result and
                result['error']['cause']['name'] == 'UNKNOWN_ACCOUNT'):
            break
    else:
        assert False, f'Account {implicit.account_id} wasn’t deleted:\n{result}'


if __name__ == '__main__':
    test_delete_implicit_account()
