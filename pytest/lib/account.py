import base58
import base64
import json
import requests
import itertools

import transaction


DEFAULT_ATTACHED_GAS = 10000000000000000


class JsonProviderError(Exception):
    pass

class JsonProvider(object):

    def __init__(self, rpc_addr):
        self._rpc_addr = rpc_addr

    def rpc_addr(self):
        return self._rpc_addr

    def json_rpc(self, method, params, timeout=2):
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        r = requests.post("http://%s:%s" % self.rpc_addr(), json=j, timeout=timeout)
        r.raise_for_status()
        content = json.loads(r.content)
        if "error" in content:
            raise JsonProviderError(content["error"])
        return content["result"]

    def send_tx(self, signed_tx):
        return self.json_rpc('broadcast_tx_async', [base64.b64encode(signed_tx).decode('utf8')])

    def send_tx_and_wait(self, signed_tx, timeout):
        return self.json_rpc('broadcast_tx_commit', [base64.b64encode(signed_tx).decode('utf8')], timeout=timeout)

    def get_status(self):
        r = requests.get("http://%s:%s/status" % self.rpc_addr(), timeout=2)
        r.raise_for_status()
        return json.loads(r.content)

    def get_validators(self):
        return self.json_rpc('validators', [None])

    def query(self, query_object):
        return self.json_rpc('query', query_object)

    def get_account(self, account_id, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "view_account", "account_id": account_id, "finality": finality})

    def get_access_key_list(self, account_id, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "view_access_key_list", "account_id": account_id, "finality": finality})

    def get_access_key(self, account_id, public_key, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "view_access_key", "account_id": account_id,
                             "public_key": public_key, "finality": finality})

    def view_call(self, account_id, method_name, args, finality='optimistic'):
        return self.json_rpc('query', {"request_type": "call_function", "account_id": account_id,
                                       "method_name": method_name, "args_base64": base64.b64encode(args).decode('utf8'), "finality": finality})

    def get_block(self, block_id):
        return self.json_rpc('block', [block_id])

    def get_chunk(self, chunk_id):
        return self.json_rpc('chunk', [chunk_id])

    def get_tx(self, tx_hash, tx_recipient_id):
        return self.json_rpc('tx', [tx_hash, tx_recipient_id])

    def get_changes_in_block(self, changes_in_block_request):
        return self.json_rpc('EXPERIMENTAL_changes_in_block', changes_in_block_request)


class TransactionError(Exception):
    pass


class ViewFunctionError(Exception):
    pass


class Account(object):

    def __init__(self, provider, signer, account_id):
        self._provider = provider
        self._signer = signer
        self._account_id = account_id
        self._account = provider.get_account(account_id)
        self._access_key = provider.get_access_key(account_id, signer.pk)

    def _sign_and_submit_tx(self, receiver_id, actions):
        self._access_key["nonce"] += 1
        block_hash = self._provider.get_status()['sync_info']['latest_block_hash']
        block_hash = base58.b58decode(block_hash.encode('utf8'))
        serialzed_tx = transaction.sign_and_serialize_transaction(
            receiver_id, self._access_key["nonce"], actions, block_hash, self._account_id,
            self._signer.decoded_pk(), self._signer.decoded_sk())
        result = self._provider.send_tx_and_wait(serialzed_tx, 10)
        for outcome in itertools.chain([result['transaction_outcome']], result['receipts_outcome']):
            for log in outcome['outcome']['logs']:
                print("Log:", log)
        if 'Failure' in result['status']:
            raise TransactionError(result['status']['Failure'])
        return result

    @property
    def account_id(self):
        return self._account_id

    @property
    def signer(self):
        return self._signer

    @property
    def provider(self):
        return self._provider

    def send_money(self, account_id, amount):
        return self._sign_and_submit_tx(account_id, [transaction.create_transfer_action(amount)])

    def function_call(self, contract_id, method_name, args, gas=DEFAULT_ATTACHED_GAS, amount=0):
        args = json.dumps(args).encode('utf8')
        return self._sign_and_submit_tx(contract_id, [transaction.create_function_call_action(method_name, args, gas, amount)])

    def create_account(self, account_id, public_key, initial_balance):
        actions = [
            transaction.create_create_account_action(),
            transaction.create_full_access_key_action(public_key),
            transaction.create_transfer_action(initial_balance)]
        return self._sign_and_submit_tx(account_id, actions)

    def deploy_contract(self, contract_code):
        return self._sign_and_submit_tx(self._account_id, [transaction.create_deploy_contract_action(contract_code)])

    def stake(self, public_key, amount):
        return self._sign_and_submit_tx(self._account_id, [transaction.create_stake_action(public_key, amount)])

    def create_and_deploy_contract(self, contract_id, public_key, contract_code, initial_balance):
        actions = [
            transaction.create_create_account_action(),
            transaction.create_transfer_action(initial_balance),
            transaction.create_deploy_contract_action(contract_code)] + \
                  ([transaction.create_full_access_key_action(public_key)] if public_key is not None else [])
        return self._sign_and_submit_tx(contract_id, actions)

    def create_deploy_and_init_contract(self, contract_id, public_key, contract_code, initial_balance, args,
                                        gas=DEFAULT_ATTACHED_GAS, init_method_name="new"):
        args = json.dumps(args).encode('utf8')
        actions = [
          transaction.create_create_account_action(),
          transaction.create_transfer_action(initial_balance),
          transaction.create_deploy_contract_action(contract_code),
          transaction.create_function_call_action(init_method_name, args, gas, 0)] + \
                  ([transaction.create_full_access_key_action(public_key)] if public_key is not None else [])
        return self._sign_and_submit_tx(contract_id, actions)

    def view_function(self, contract_id, method_name, args):
        result = self._provider.view_call(contract_id, method_name, json.dumps(args).encode('utf8'))
        if "error" in result:
            raise ViewFunctionError(result["error"])
        result["result"] = json.loads(''.join([chr(x) for x in result["result"]]))
        return result
