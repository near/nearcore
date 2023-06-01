import base64
import json
import requests
import random
import time

from transaction import (
    sign_payment_tx, sign_deploy_contract_tx, sign_function_call_tx,
    sign_create_account_with_full_access_key_and_balance_tx, sign_staking_tx)
from key import Key
from utils import load_binary_file
from configured_logger import logger

# Constant for 1 NEAR
NEAR_BASE = 10**24
TGAS = 10**12


class Account:

    def __init__(self,
                 key,
                 init_nonce,
                 base_block_hash,
                 rpc_info=None,
                 rpc_infos=None):
        """
        `rpc_info` takes precedence over `rpc_infos` for compatibility. One of them must be set.
        If `rpc_info` is set, only this RPC node will be contacted.
        Otherwise if `rpc_infos` is set, an RPC node will be selected randomly
        from that set for each transaction attempt.
        """
        self.key = key
        self.nonce = init_nonce
        self.base_block_hash = base_block_hash
        assert rpc_info or rpc_infos
        if rpc_info:
            assert not rpc_infos
            rpc_infos = [rpc_info]
        self.rpc_infos = rpc_infos
        assert key.account_id
        self.tx_timestamps = []
        logger.debug(
            f'Creating Account {key.account_id} {init_nonce} {self.rpc_infos[0]} {key.pk} {key.sk}'
        )

    # Returns an address of a random known RPC node.
    def get_rpc_node_address(self):
        rpc_addr, rpc_port = random.choice(self.rpc_infos)
        return f'http://{rpc_addr}:{rpc_port}'

    def json_rpc(self, method, params):
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        r = requests.post(self.get_rpc_node_address(), json=j, timeout=30)
        return json.loads(r.content)

    def send_tx(self, signed_tx):
        return self.json_rpc('broadcast_tx_async',
                             [base64.b64encode(signed_tx).decode('utf-8')])

    def send_tx_sync(self, signed_tx):
        return self.json_rpc('broadcast_tx_commit',
                             [base64.b64encode(signed_tx).decode('utf-8')])

    def prep_tx(self):
        self.tx_timestamps.append(time.time())
        self.nonce += 1

    def send_transfer_tx(self,
                         dest_account_id,
                         transfer_amount=100,
                         base_block_hash=None):
        self.prep_tx()
        tx = sign_payment_tx(self.key, dest_account_id, transfer_amount,
                             self.nonce, base_block_hash or
                             self.base_block_hash)
        return self.send_tx(tx)

    def send_deploy_contract_tx(self, wasm_filename, base_block_hash=None):
        wasm_binary = load_binary_file(wasm_filename)
        self.prep_tx()
        tx = sign_deploy_contract_tx(self.key, wasm_binary, self.nonce,
                                     base_block_hash or self.base_block_hash)
        return self.send_tx(tx)

    def send_call_contract_tx(self, method_name, args, base_block_hash=None):
        return self.send_call_contract_raw_tx(self.key.account_id,
                                              method_name,
                                              args,
                                              0,
                                              base_block_hash=base_block_hash)

    def send_call_contract_raw_tx(self,
                                  contract_id,
                                  method_name,
                                  args,
                                  deposit,
                                  base_block_hash=None):
        self.prep_tx()
        tx = sign_function_call_tx(self.key, contract_id, method_name, args,
                                   300 * TGAS, deposit, self.nonce,
                                   base_block_hash or self.base_block_hash)
        return self.send_tx(tx)

    def send_call_contract_raw_tx_sync(self,
                                       contract_id,
                                       method_name,
                                       args,
                                       deposit,
                                       base_block_hash=None):
        self.prep_tx()
        tx = sign_function_call_tx(self.key, contract_id, method_name, args,
                                   300 * TGAS, deposit, self.nonce,
                                   base_block_hash or self.base_block_hash)
        return self.send_tx_sync(tx)

    def send_create_account_tx(self, new_account_id, base_block_hash=None):
        self.prep_tx()
        new_key = Key(new_account_id, self.key.pk, self.key.sk)
        tx = sign_create_account_with_full_access_key_and_balance_tx(
            self.key, new_account_id, new_key, 100 * NEAR_BASE, self.nonce,
            base_block_hash or self.base_block_hash)
        return self.send_tx(tx)

    def send_stake_tx(self, stake_amount, base_block_hash=None):
        self.prep_tx()
        tx = sign_staking_tx(self.key, self.key, stake_amount, self.nonce,
                             base_block_hash or self.base_block_hash)
        return self.send_tx(tx)

    def get_amount_yoctonear(self):
        j = self.json_rpc(
            'query', {
                'request_type': 'view_account',
                'finality': 'optimistic',
                'account_id': self.key.account_id
            })
        return int(j.get('result', {}).get('amount', 0))
