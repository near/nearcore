"""ML-DSA-65 (post-quantum) keys, mirroring `SecretKey::MLDSA65` in core/crypto.

Needs cryptography>=48, so only import this from tests that use ML-DSA-65 keys.
"""

import hashlib
import typing

import base58
from cryptography.hazmat.primitives.asymmetric import mldsa

from messages.crypto import KEY_TYPE_MLDSA65

SEED_LENGTH = 32
# `HashDomainTag::MlDsa65PubkeyV1` in core/crypto.
HASH_DOMAIN_TAG = b'near:ml-dsa-65-pubkey-hash:v1'
PUBLIC_KEY_PREFIX = 'ml-dsa-65:'
HANDLE_PREFIX = 'ml-dsa-65-hash:'


def _b58(data: bytes) -> str:
    return base58.b58encode(data).decode('ascii')


class MlDsa65Key:
    """An ML-DSA-65 signer, shaped like `key.Key`.

    `pk` is the on-trie handle, because that is what the chain indexes access
    keys by, while `full_pk` is the 1952-byte key that goes on the wire.
    `decoded_sk()` is the 32-byte seed rather than the expanded private key;
    it is all that is needed to sign.
    """
    key_type = KEY_TYPE_MLDSA65

    def __init__(self, account_id: str, seed: bytes) -> None:
        self.account_id = account_id
        self.seed = seed
        self._private_key = self._load(seed)
        self.pubkey = self._private_key.public_key().public_bytes_raw()
        self.pk = HANDLE_PREFIX + _b58(self.handle_digest())
        self.full_pk = PUBLIC_KEY_PREFIX + _b58(self.pubkey)

    @classmethod
    def from_seed_testonly(cls, account_id: str, seed: str) -> 'MlDsa65Key':
        """Derive from a seed string, padded like `SecretKey::from_seed` does."""
        return cls(account_id,
                   seed.encode('utf8')[:SEED_LENGTH].ljust(SEED_LENGTH, b' '))

    @classmethod
    def sign_with_seed(cls, seed: bytes,
                       data: typing.Union[bytes, bytearray]) -> bytes:
        """Sign without building a key: the generic `transaction.sign_hash`
        path only has the raw secret key."""
        return cls._load(seed).sign(bytes(data))

    @staticmethod
    def _load(seed: bytes) -> mldsa.MLDSA65PrivateKey:
        assert len(seed) == SEED_LENGTH, \
            f'ML-DSA-65 seed must be {SEED_LENGTH} bytes, got {len(seed)}'
        return mldsa.MLDSA65PrivateKey.from_seed_bytes(seed)

    def handle_digest(self) -> bytes:
        """SHA3-256 of (domain tag || pubkey), the raw on-trie handle bytes."""
        digest = hashlib.sha3_256(HASH_DOMAIN_TAG + self.pubkey).digest()
        assert len(digest) == SEED_LENGTH
        return digest

    def decoded_pk(self) -> bytes:
        return self.pubkey

    def decoded_sk(self) -> bytes:
        return self.seed

    def sign_bytes(self, data: typing.Union[bytes, bytearray]) -> bytes:
        return self._private_key.sign(bytes(data))
