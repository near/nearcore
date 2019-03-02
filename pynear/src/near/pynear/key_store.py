import json
import os

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from near.pynear import b58


class AmbiguousPublicKey(Exception):
    def __init__(self):
        msg = 'public key must be specified if there is more ' \
              'than one key in the key store'
        super(AmbiguousPublicKey, self).__init__(msg)


class NoKeyPairs(Exception):
    pass


class KeyStore(object):
    @staticmethod
    def _create_key_pair(seed=None):
        if seed is not None:
            if len(seed) > 32:
                raise Exception('max seed length is 32')

            seed = seed.encode('utf-8')
            seed = seed.ljust(32)
            key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
        else:
            key = ed25519.Ed25519PrivateKey.generate()

        return key, key.public_key()

    @staticmethod
    def _get_b58_encoded_public_key(public_key):
        public_key = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        return b58.b58encode(public_key).decode('utf-8')

    def create_key_pair(self, seed=None):
        raise NotImplementedError

    def sign(self, data, public_key=None):
        raise NotImplementedError

    def get_only_public_key(self):
        raise NotImplementedError


class InMemoryKeyStore(KeyStore):
    def __init__(self):
        self._key_pairs = {}

    def create_key_pair(self, seed=None):
        (secret_key, public_key) = self._create_key_pair(seed)
        encoded = self._get_b58_encoded_public_key(public_key)
        self._key_pairs[encoded] = secret_key
        return encoded

    def sign(self, data, public_key=None):
        if public_key is None:
            public_key = self.get_only_public_key()

        secret_key = self._key_pairs[public_key]
        return secret_key.sign(data)

    def get_only_public_key(self):
        if len(self._key_pairs) > 1:
            raise AmbiguousPublicKey
        elif len(self._key_pairs) == 0:
            raise NoKeyPairs

        return list(self._key_pairs.keys())[0]


class FileKeyStore(KeyStore):
    def __init__(self, path):
        self._path = path

    def create_key_pair(self, seed=None):
        if not os.path.exists(self._path):
            os.makedirs(self._path)

        (secret_key, public_key) = self._create_key_pair(seed)
        encoded_pub = self._get_b58_encoded_public_key(public_key)

        secret_key = secret_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        encoded_secret = b58.b58encode(secret_key).decode('utf-8')

        with open(os.path.join(self._path, encoded_pub), 'w') as f:
            key_file = {
                'public_key': encoded_pub,
                'secret_key': encoded_secret,
            }
            f.write(json.dumps(key_file))

        return encoded_pub

    def sign(self, data, public_key=None):
        if public_key is None:
            public_key = self.get_only_public_key()

        with open(os.path.join(self._path, public_key)) as f:
            key_file = json.loads(f.read())
            encoded_secret = key_file['secret_key']

        secret_key = b58.b58decode(encoded_secret)
        secret_key = ed25519.Ed25519PrivateKey.from_private_bytes(secret_key)
        return secret_key.sign(data)

    def get_only_public_key(self):
        if not os.path.exists(self._path):
            raise NoKeyPairs

        pub_keys = os.listdir(self._path)
        if len(pub_keys) > 1:
            raise AmbiguousPublicKey
        elif len(pub_keys) == 0:
            raise NoKeyPairs

        return pub_keys[0]
