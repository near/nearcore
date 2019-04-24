from __future__ import print_function

import base64
import hashlib
import json

import requests

from near.pynear import b58
from near.pynear.key_store import InMemoryKeyStore
from near.pynear.protos import signed_transaction_pb2

try:
    # py2
    from urllib2 import urlopen, Request, HTTPError, URLError
except ImportError:
    # py3
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError


# Data is empty string instead of None because method is
# defined by whether or not data is None and cannot be
# specified otherwise in py2
def _post(url, data=''):
    if data != '':
        data = json.dumps(data).encode('utf-8')

    request = Request(url, data=data)

    if data is not None:
        request.add_header('Content-Type', 'application/json')

    connection = urlopen(request)
    return connection


class NearLib(object):
    def __init__(
            self,
            server_url='http://localhost:3030/',
            keystore=None,
            public_key=None,
            debug=False,
    ):
        self._server_url = server_url
        if keystore is None:
            keystore = InMemoryKeyStore()
        self.keystore = keystore
        self._public_key = public_key
        self._nonces = {}
        self._debug = debug

    def _get_nonce(self, originator):
        if originator not in self._nonces:
            view_result = self.view_account(originator)
            self._nonces[originator] = view_result.get('nonce', 0) + 1

        return self._nonces[originator]

    def _update_nonce(self, originator):
        self._nonces[originator] += 1

    def _call_rpc(self, method_name, params=None):
        data = params
        if self._debug:
            print(data)

        try:
            connection = _post(self._server_url + method_name, data)
            raw = connection.read()
            if self._debug:
                print(raw)
            return json.loads(raw.decode('utf-8'))
        except HTTPError as e:
            if e.code == 400:
                raise Exception(e.fp.read())
            raise
        except URLError:
            error = "Connection to {} refused. " \
                    "To start RPC server at http://127.0.0.1:3030, run:\n" \
                    "cargo run -p devnet"
            raise Exception(error.format(self._server_url))

    def _sign_transaction_body(self, body):
        body = body.SerializeToString()
        m = hashlib.sha256()
        m.update(body)
        data = m.digest()
        return self.keystore.sign(data, self._public_key)

    def _submit_transaction(self, transaction):
        transaction = transaction.SerializeToString()
        transaction = base64.b64encode(transaction).decode('utf-8')
        params = {'transaction': transaction}
        return self._call_rpc('submit_transaction', params)

    def _get_public_key(self):
        if self._public_key is None:
            self._public_key = self.keystore.get_only_public_key()

        return self._public_key

    def deploy_contract(self, contract_name, wasm_file):
        with open(wasm_file, 'rb') as f:
            wasm_byte_array = f.read()

        nonce = self._get_nonce(contract_name)

        deploy_contract = signed_transaction_pb2.DeployContractTransaction()
        deploy_contract.nonce = nonce
        deploy_contract.contract_id = contract_name
        deploy_contract.wasm_byte_array = wasm_byte_array

        signature = self._sign_transaction_body(deploy_contract)

        signed_transaction = signed_transaction_pb2.SignedTransaction()
        signed_transaction.deploy_contract.CopyFrom(deploy_contract)
        signed_transaction.signature = signature

        self._update_nonce(contract_name)
        return self._submit_transaction(signed_transaction)

    def send_money(self, originator, receiver, amount):
        nonce = self._get_nonce(originator)

        send_money = signed_transaction_pb2.SendMoneyTransaction()
        send_money.nonce = nonce
        send_money.originator = originator
        send_money.receiver = receiver
        send_money.amount = amount

        signature = self._sign_transaction_body(send_money)

        signed_transaction = signed_transaction_pb2.SignedTransaction()
        signed_transaction.send_money.CopyFrom(send_money)
        signed_transaction.signature = signature

        self._update_nonce(originator)
        return self._submit_transaction(signed_transaction)

    def stake(self, originator, amount):
        nonce = self._get_nonce(originator)

        stake = signed_transaction_pb2.StakeTransaction()
        stake.nonce = nonce
        stake.originator = originator
        stake.amount = amount

        signature = self._sign_transaction_body(stake)

        signed_transaction = signed_transaction_pb2.SignedTransaction()
        signed_transaction.stake.CopyFrom(stake)
        signed_transaction.signature = signature

        self._update_nonce(originator)
        return self._submit_transaction(signed_transaction)

    def schedule_function_call(
            self,
            originator,
            contract_name,
            method_name,
            amount,
            args=None,
    ):
        if args is None:
            args = "{}"

        nonce = self._get_nonce(originator)
        function_call = signed_transaction_pb2.FunctionCallTransaction()
        function_call.nonce = nonce
        function_call.originator = originator
        function_call.contract_id = contract_name
        function_call.method_name = method_name.encode('utf-8')
        function_call.args = args.encode('utf-8')
        function_call.amount = amount

        signature = self._sign_transaction_body(function_call)

        signed_transaction = signed_transaction_pb2.SignedTransaction()
        signed_transaction.function_call.CopyFrom(function_call)
        signed_transaction.signature = signature

        self._update_nonce(originator)
        return self._submit_transaction(signed_transaction)

    def view_state(self, contract_name):
        params = {'contract_account_id': contract_name}
        return self._call_rpc('view_state', params)

    def view_latest_beacon_block(self):
        return self._call_rpc('view_latest_beacon_block')

    def get_beacon_block_by_hash(self, _hash):
        params = {'hash': _hash}
        return self._call_rpc('get_beacon_block_by_hash', params)

    def view_latest_shard_block(self):
        return self._call_rpc('view_latest_shard_block')

    def get_shard_block_by_hash(self, _hash):
        params = {'hash': _hash}
        return self._call_rpc('get_shard_block_by_hash', params)

    def list_beacon_blocks(self, start=None, limit=None):
        params = {
            'start': start,
            'limit': limit,
        }
        return self._call_rpc('get_beacon_blocks_by_index', params)

    def create_account(
            self,
            originator,
            account_id,
            amount,
            account_public_key,
    ):
        if not account_public_key:
            account_public_key = self._get_public_key()

        nonce = self._get_nonce(originator)

        create_account = signed_transaction_pb2.CreateAccountTransaction()
        create_account.nonce = nonce
        create_account.originator = originator
        create_account.new_account_id = account_id
        create_account.amount = amount
        create_account.public_key = b58.b58decode(account_public_key)

        signature = self._sign_transaction_body(create_account)

        signed_transaction = signed_transaction_pb2.SignedTransaction()
        signed_transaction.create_account.CopyFrom(create_account)
        signed_transaction.signature = signature

        self._update_nonce(originator)
        return self._submit_transaction(signed_transaction)

    def swap_key(
            self,
            account,
            current_key,
            new_key,
    ):
        nonce = self._get_nonce(account)

        swap_key = signed_transaction_pb2.SwapKeyTransaction()
        swap_key.nonce = nonce
        swap_key.originator = account
        swap_key.cur_key = b58.b58decode(current_key)
        swap_key.new_key = b58.b58decode(new_key)

        signature = self._sign_transaction_body(swap_key)

        signed_transaction = signed_transaction_pb2.SignedTransaction()
        signed_transaction.swap_key.CopyFrom(swap_key)
        signed_transaction.signature = signature

        self._update_nonce(account)
        return self._submit_transaction(signed_transaction)

    def view_account(self, account_id):
        params = {
            'account_id': account_id,
        }
        return self._call_rpc('view_account', params)

    def call_view_function(
            self,
            contract_name,
            function_name,
            args=None,
    ):
        if args is None:
            args = "{}"
        args = list(bytearray(args, 'utf-8'))

        params = {
            'contract_account_id': contract_name,
            'method_name': function_name,
            'args': args,
        }
        result = self._call_rpc('call_view_function', params)
        try:
            return json.loads(bytearray(result['result']).decode('utf-8'))
        except json.JSONDecodeError:
            return result

    def check_health(self):
        url = "{}status".format(self._server_url)
        response = requests.get(url)
        return response.status_code == 200

    def get_transaction_result(self, hash_):
        params = {'hash': hash_}
        return self._call_rpc('get_transaction_result', params)

    def get_contract_info(self, contract_account_id):
        params = {'contract_account_id': contract_account_id}
        return self._call_rpc('view_state', params)

    def generate_key_pair(self, seed):
        return self.keystore.create_key_pair(seed)
