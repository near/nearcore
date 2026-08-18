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


# Borsh variant names for PublicKey. A key's position in `PublicKey`'s schema
# below is its borsh discriminant, and the name doubles as the attribute the
# serializer reads the key bytes from.
KEY_TYPE_ED25519 = 'ed25519'
KEY_TYPE_SECP256K1 = 'secp256k1'
KEY_TYPE_MLDSA65 = 'mldsa65'

KEY_TYPE_LENGTHS = {
    KEY_TYPE_ED25519: 32,
    KEY_TYPE_SECP256K1: 64,
    KEY_TYPE_MLDSA65: 1952,
}


class PublicKey:
    pass


def make_public_key(data: bytes, key_type: str = KEY_TYPE_ED25519) -> PublicKey:
    """Build a PublicKey from raw key bytes."""
    expected = KEY_TYPE_LENGTHS[key_type]
    assert len(data) == expected, \
        f'{key_type} public key must be {expected} bytes, got {len(data)}'
    public_key = PublicKey()
    public_key.enum = key_type
    setattr(public_key, key_type, data)
    return public_key


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
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                [KEY_TYPE_ED25519, [KEY_TYPE_LENGTHS[KEY_TYPE_ED25519]]],
                [KEY_TYPE_SECP256K1, [KEY_TYPE_LENGTHS[KEY_TYPE_SECP256K1]]],
                [KEY_TYPE_MLDSA65, [KEY_TYPE_LENGTHS[KEY_TYPE_MLDSA65]]],
            ]
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
