import typing

import base58

# Borsh variant names for PublicKey and Signature. A key's position in the
# `PublicKey` / `Signature` schemas below is its borsh discriminant, and the
# name doubles as the attribute the serializer reads the bytes from.
KEY_TYPE_ED25519 = 'ed25519'
KEY_TYPE_SECP256K1 = 'secp256k1'
KEY_TYPE_MLDSA65 = 'mldsa65'

KEY_TYPE_LENGTHS = {
    KEY_TYPE_ED25519: 32,
    KEY_TYPE_SECP256K1: 64,
    KEY_TYPE_MLDSA65: 1952,
}

SIGNATURE_LENGTHS = {
    KEY_TYPE_ED25519: 64,
    KEY_TYPE_SECP256K1: 65,
    KEY_TYPE_MLDSA65: 3309,
}

# Prefix each key type is displayed with in strings like `ed25519:3s2f...`.
KEY_TYPE_BY_PREFIX = {
    'ed25519': KEY_TYPE_ED25519,
    'secp256k1': KEY_TYPE_SECP256K1,
    'ml-dsa-65': KEY_TYPE_MLDSA65,
}


class PublicKey:
    pass


class Signature:

    def __init__(self, signature: typing.Optional[str] = None) -> None:
        if signature:
            prefix, data = signature.split(':')
            init_key_enum(self, base58.b58decode(data),
                          KEY_TYPE_BY_PREFIX[prefix], SIGNATURE_LENGTHS,
                          'signature')


def init_key_enum(obj, data: bytes, key_type: str, lengths, what: str):
    """Set the borsh enum variant and its raw bytes on obj."""
    expected = lengths[key_type]
    assert len(data) == expected, \
        f'{key_type} {what} must be {expected} bytes, got {len(data)}'
    obj.enum = key_type
    setattr(obj, key_type, data)
    return obj


def make_public_key(data: bytes, key_type: str = KEY_TYPE_ED25519) -> PublicKey:
    """Build a PublicKey from raw key bytes."""
    return init_key_enum(PublicKey(), data, key_type, KEY_TYPE_LENGTHS,
                         'public key')


def make_signature(data: bytes, key_type: str = KEY_TYPE_ED25519) -> Signature:
    """Build a Signature from raw signature bytes."""
    return init_key_enum(Signature(), data, key_type, SIGNATURE_LENGTHS,
                         'signature')


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
            'kind':
                'enum',
            'field':
                'enum',
            'values': [
                [KEY_TYPE_ED25519, [SIGNATURE_LENGTHS[KEY_TYPE_ED25519]]],
                [KEY_TYPE_SECP256K1, [SIGNATURE_LENGTHS[KEY_TYPE_SECP256K1]]],
                [KEY_TYPE_MLDSA65, [SIGNATURE_LENGTHS[KEY_TYPE_MLDSA65]]],
            ]
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
