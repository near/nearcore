import json
import os

import ed25519

import base64

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

        kwargs = {}
        if seed is not None:
            kwargs['entropy'] = lambda x: seed.ljust(32)

        return ed25519.create_keypair(**kwargs)

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
        encoded = base64.b64encode(public_key.to_bytes()).decode('utf-8')
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
        keys = json.loads(open(path).read())
        self.public_key = keys['public_key']
        self.secret_key = ed25519.SigningKey(base64.b64decode(keys['secret_key']))

    def sign(self, data, public_key=None):
        return self.secret_key.sign(data)

    def get_only_public_key(self):
        return self.public_key

    def verify(self, signature, data):
        return ed25519.VerifyingKey(base64.b64decode(self.public_key)).verify(signature, data)

