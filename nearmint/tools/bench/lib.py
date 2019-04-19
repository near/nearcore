import json
import base64
import hashlib
import time
import random
import collections

try:
    from urllib2 import urlopen, Request, HTTPError, URLError, quote
except ImportError:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError
    from urllib.parse import quote

import protos.signed_transaction_pb2 as signed_transaction
from key_store import InMemoryKeyStore


def _post(url, data=''):
    if data != '':
        data = json.dumps(data).encode('utf-8')
    
    request = Request(url, data=data)

    if data is not None:
        request.add_header('Content-Type', 'application/json')

    connection = urlopen(request)
    return connection


class RPC(object):

    def __init__(self, server_url):
        self._server_url = server_url

    def _call_rpc(self, method_name, params):
        data = {
            'method': method_name,
            'jsonrpc': '2.0',
            'params': params,
            'id': 'dontcare'
        }
        try:
            connection = _post(self._server_url, data)
            raw = connection.read()
            return json.loads(raw.decode('utf-8'))
        except HTTPError as e:
            if e.code == 400:
                raise Exception(e.fp.read())
            raise
        except URLError:
            error = "Connection to {} refused. "
            raise

    def query(self, path, args=''):
        result = self._call_rpc('abci_query', [path, args, '0', False])
        return result['result']['response']

    def send_transaction(self, transaction, wait=False):
        data = transaction.SerializeToString()
        data = base64.b64encode(data).decode('utf8')
        result = self._call_rpc('broadcast_tx_commit' if wait else 'broadcast_tx_async', [data])
        return result

    def get_header(self, index):
        return self._call_rpc('block', [str(index)])

    def status(self):
        return self._call_rpc('status', [])

    def get_account(self, account_id):
        response = rpc.query('account/%s' % account_id)
        return json.loads(base64.b64decode(response['value']))

    def call_function(self, contract_id, method_name, args):
        return self.query('call/%s/%s' % (contract_id, method_name), args.encode('hex'))


class User(object):

    def __init__(self, rpc, account_id, public_key=None, keystore=None):
        self._rpc = rpc
        self._account_id = account_id
        self._nonce = rpc.get_account(account_id)['nonce']
        if keystore is None:
            keystore = InMemoryKeyStore()
        self._keystore = keystore
        if public_key is None:
            public_key = keystore.create_key_pair(seed=account_id)
        self._public_key = public_key

    @property
    def account_id(self):
        return self._account_id

    def view_account(self):
        return self._rpc.get_account(self._account_id)

    def _sign_transaction_body(self, body):
        body = body.SerializeToString()
        m = hashlib.sha256()
        m.update(body)
        data = m.digest()
        return self._keystore.sign(data, self._public_key)

    def create_user(self, account_id, amount):
        raise NotImplemented

    def send_money(self, receiver, amount, wait=False):
        self._nonce += 1
        send_money = signed_transaction.SendMoneyTransaction()
        send_money.nonce = self._nonce
        send_money.originator = self._account_id
        send_money.receiver = receiver
        send_money.amount = amount

        signature = self._sign_transaction_body(send_money)

        transaction = signed_transaction.SignedTransaction()
        transaction.send_money.CopyFrom(send_money)
        transaction.signature = signature
        
        return self._rpc.send_transaction(transaction, wait=wait)


if __name__ == "__main__":
    rpc = RPC('http://localhost:3030/')
    alice = User(rpc, "alice.near")
    print(alice.view_account())
    print(alice.send_money('bob.near', 10, wait=True))
