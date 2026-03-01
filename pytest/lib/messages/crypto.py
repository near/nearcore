import typing

import base58


class Signature:
    _KEY_TYPES = {
        'ed25519': 0,
        'secp256k1': 1,
    }

    def __init__(self, signature: typing.Optional[str] = None) -> None:
        if signature:
            keyType, data = signature.split(':')
            self.keyType = self._KEY_TYPES[keyType]
            self.data = base58.b58decode(data)


class PublicKey:
    pass


class AccessKey:
    pass


class AccessKeyPermission:
    pass


class FunctionCallPermission:
    pass


class FullAccessPermission:
    pass


class GasKeyInfo:
    pass


class GasKeyFunctionCallPermission:
    pass


class GasKeyFullAccessPermission:
    pass


class Direction:
    pass


class MerklePath:
    pass


class ShardProof:
    pass


crypto_schema = [
    [
        Signature, {
            'kind': 'struct',
            'fields': [['keyType', 'u8'], ['data', [64]]]
        }
    ],
    [
        PublicKey, {
            'kind': 'struct',
            'fields': [['keyType', 'u8'], ['data', [32]]]
        }
    ],
    [
        AccessKey, {
            'kind': 'struct',
            'fields': [
                ['nonce', 'u64'],
                ['permission', AccessKeyPermission],
            ]
        }
    ],
    [
        AccessKeyPermission, {
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                ['functionCall', FunctionCallPermission],
                ['fullAccess', FullAccessPermission],
                ['gasKeyFunctionCall', GasKeyFunctionCallPermission],
                ['gasKeyFullAccess', GasKeyFullAccessPermission],
            ]
        }
    ],
    [
        FunctionCallPermission, {
            'kind':
                'struct',
            'fields': [
                ['allowance', {
                    'kind': 'option',
                    'type': 'u128'
                }],
                ['receiverId', 'string'],
                ['methodNames', ['string']],
            ]
        }
    ],
    [FullAccessPermission, {
        'kind': 'struct',
        'fields': []
    }],
    [
        GasKeyInfo, {
            'kind': 'struct',
            'fields': [['balance', 'u128'], ['numNonces', 'u16']]
        }
    ],
    [
        GasKeyFunctionCallPermission, {
            'kind':
                'struct',
            'fields': [
                ['gasKeyInfo', GasKeyInfo],
                ['functionCallPermission', FunctionCallPermission],
            ]
        }
    ],
    [
        GasKeyFullAccessPermission, {
            'kind': 'struct',
            'fields': [['gasKeyInfo', GasKeyInfo]]
        }
    ],
    [
        Direction, {
            'kind': 'enum',
            'field': 'enum',
            'values': [['Left', ()], ['Right', ()]],
        }
    ],
    [MerklePath, {
        'kind': 'struct',
        'fields': [['f1', [([32], Direction)]]],
    }],
    [
        ShardProof, {
            'kind':
                'struct',
            'fields': [['from_shard_id', 'u64'], ['to_shard_id', 'u64'],
                       ['proof', MerklePath]],
        }
    ],
]
