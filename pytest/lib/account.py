import base64
import json
import requests
import time

from cluster import Key
from transaction import (
    sign_payment_tx, sign_deploy_contract_tx, sign_function_call_tx,
    sign_create_account_with_full_access_key_and_balance_tx, sign_staking_tx)
import utils


class Account:

    def __init__(self, key, init_nonce, base_block_hash, rpc_info):
        self.key = key
        self.nonce = init_nonce
        self.base_block_hash = base_block_hash
        self.rpc_addr, self.rpc_port = rpc_info
        self.tx_timestamps = []

    def json_rpc(self, method, params):
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        r = requests.post(f'http://{self.rpc_addr}:{self.rpc_port}',
                          json=j,
                          timeout=10)
        return json.loads(r.content)

    def send_tx(self, signed_tx):
        return self.json_rpc('broadcast_tx_async',
                             [base64.b64encode(signed_tx).decode('utf8')])

    def prep_tx(self):
        self.tx_timestamps.append(time.time())
        self.nonce += 1

    def send_transfer_tx(self, dest_account_id):
        self.prep_tx()
        transfer_amount = 100
        tx = sign_payment_tx(self.key, dest_account_id, transfer_amount,
                             self.nonce, self.base_block_hash)
        return self.send_tx(tx)

    def send_deploy_contract_tx(self, wasm_filename):
        wasm_binary = utils.load_binary_file(wasm_filename)
        self.prep_tx()
        tx = sign_deploy_contract_tx(self.key, wasm_binary, self.nonce,
                                     self.base_block_hash)
        return self.send_tx(tx)

    def send_call_contract_tx(self, method_name, args):
        self.prep_tx()
        tx = sign_function_call_tx(self.key, self.key.account_id, method_name,
                                   args, 300000000000000, 0, self.nonce,
                                   self.base_block_hash)
        return self.send_tx(tx)

    def send_create_account_tx(self, new_account_id):
        self.prep_tx()
        new_key = Key(new_account_id, self.key.pk, self.key.sk)
        tx = sign_create_account_with_full_access_key_and_balance_tx(
            self.key, new_account_id, new_key, 100, self.nonce,
            self.base_block_hash)
        return self.send_tx(tx)

    def send_stake_tx(self, stake_amount):
        self.prep_tx()
        tx = sign_staking_tx(self.key, self.key, stake_amount, self.nonce,
                             self.base_block_hash)
        return self.send_tx(tx)
