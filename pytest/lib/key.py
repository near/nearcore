import base58
import json


class Key(object):

    def __init__(self, account_id, pk, sk):
        super(Key, self).__init__()
        self.account_id = account_id
        self.pk = pk
        self.sk = sk

    def decoded_pk(self):
        key = self.pk.split(':')[1] if ':' in self.pk else self.pk
        return base58.b58decode(key.encode('ascii'))

    def decoded_sk(self):
        key = self.sk.split(':')[1] if ':' in self.sk else self.sk
        return base58.b58decode(key.encode('ascii'))

    @classmethod
    def from_json(self, j):
        return Key(j['account_id'], j['public_key'], j['secret_key'])

    @classmethod
    def from_json_file(self, jf):
        with open(jf) as f:
            return Key.from_json(json.loads(f.read()))

    def to_json(self):
        return {
            'account_id': self.account_id,
            'public_key': self.pk,
            'secret_key': self.sk
        }
