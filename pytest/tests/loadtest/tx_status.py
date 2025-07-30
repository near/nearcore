import base64
import hashlib
import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from account import NEAR_BASE, TGAS
from key import Key
from argparse import ArgumentParser, Action
from urllib.parse import urlparse
from configured_logger import logger
from transaction import sign_deploy_contract_tx, sign_function_call_tx, sign_create_account_with_full_access_key_and_balance_tx
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc
from utils import load_test_contract

if __name__ == '__main__':
    parser = ArgumentParser(
        description=
        'Global contract deploy')
    #parser.add_argument('--key-path', type=str, required=True)
    parser.add_argument('--node-url', type=str, required=True)

    args = parser.parse_args()
    #signer_key = Key.from_json_file(args.key_path)
    signer_key = Key("test-user.astro-stakers.poolv1.near","ed25519:GToqd4CouqUdhexNkRWiz4y6yZi2HoNrhk5ZGLogJJF", "ed25519:3Pj3iVURbj5zyMaur6DTxNzNzZGYtuoBePvxzoovpAw4p6aDTT8Dkdg4DanrV3uVXnHH2g96hnfXaj3AwRAUKtew")
    url_parse_result = urlparse(args.node_url)
    rpc_addr, rpc_port = url_parse_result.hostname, url_parse_result.port
    latest_block_hash = get_latest_block_hash(rpc_addr, rpc_port)
    nonce = get_nonce_for_key(signer_key, addr=rpc_addr, port=rpc_port)
    test_contract = load_test_contract()

    def deploy():
        deploy_tx = sign_deploy_contract_tx(signer_key, test_contract, nonce + 1, latest_block_hash)
        resp = json_rpc('broadcast_tx_commit',
                        [base64.b64encode(deploy_tx).decode('utf8')],
                        rpc_addr, rpc_port)
        print(resp)

    def check():
        print(signer_key.account_id)
        resp = json_rpc(
            'query',
            {
                'request_type': 'view_account',
                'account_id': signer_key.account_id,
                'finality': 'optimistic'
            },
            rpc_addr,
            rpc_port
        )
        print(resp)

    def call_cross_contract():
        args = f'{{"account_id": "{signer_key.account_id}", "method_name": "ext_sha256",  "total_args_size": 1}}'
        ccc_function_call_tx = sign_function_call_tx(
            signer_key, signer_key.account_id, 'generate_large_receipt',
            bytes(args, 'utf-8'), 300000000000000, 0, nonce + 1,
            latest_block_hash)
        params = {
            'signed_tx_base64': base64.b64encode(ccc_function_call_tx).decode('utf8'),
            "wait_until": 'EXECUTED_OPTIMISTIC'
        }
        call_res = json_rpc('send_tx',
                        params,
                        rpc_addr,
                        rpc_port)
        tx_hash = call_res['result']['transaction']['hash']
        res = json_rpc('EXPERIMENTAL_tx_status', [tx_hash, signer_key.account_id], rpc_addr, rpc_port)
        function_call_receipts = [
            r for r in res['result']['receipts']
            if 'Action' in r['receipt'] and any(
                'FunctionCall' in action
                for action in r['receipt']['Action']['actions'])
        ]
        assert len(function_call_receipts) == 1, function_call_receipts
        assert function_call_receipts[0]['receipt']['Action']['actions'][0][
            'FunctionCall']['method_name'] == 'ext_sha256', function_call_receipts
        # Also check the outcome for the receipt is included and was successful
        receipt_id = function_call_receipts[0]['receipt_id']
        outcome = next(outcome['outcome']
                    for outcome in res['result']['receipts_outcome']
                    if outcome['id'] == receipt_id)
        assert "SuccessValue" in outcome['status']
    
    def test_call():
        call_tx = sign_function_call_tx(signer_key, signer_key.account_id, 'log_something',
                                [], 150 * TGAS, 1, nonce+1, latest_block_hash)
        params = {
            'signed_tx_base64': base64.b64encode(call_tx).decode('utf8'),
            "wait_until": 'EXECUTED_OPTIMISTIC'
        }
        resp = json_rpc('send_tx',
                        params,
                        rpc_addr,
                        rpc_port)
        print(resp)
        print(resp['result']['receipts_outcome'][0]['outcome']['logs'][0])
    
    def create_account():
        key = Key.from_json_file("/Users/pugachag/Data/keys/forknet_user.json")
        print(f"Create {key.account_id}")
        tx = sign_create_account_with_full_access_key_and_balance_tx(signer_key, key.account_id, key, 10 * NEAR_BASE, nonce + 1, latest_block_hash)
        resp = json_rpc('broadcast_tx_commit',
                        [base64.b64encode(tx).decode('utf8')],
                        rpc_addr, rpc_port)
        print(resp)
    
    call_cross_contract()
