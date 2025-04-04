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
from transaction import sign_deploy_global_contract_tx, sign_function_call_tx, sign_use_global_contract_tx, sign_create_account_with_full_access_key_and_balance_tx
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc
from messages.tx import GlobalContractIdentifier, GlobalContractDeployMode
from utils import load_test_contract

receiver_accounts = [
    "0-global-contract.near",
    "aurora",
    "aurora-0.near",
    "game.hot.tg",
    "kkuuue2akv_1630967379.near",
    "tge-lockup.sweat",
]

if __name__ == '__main__':
    parser = ArgumentParser(
        description=
        'Global contract deploy')
    parser.add_argument('--key-path', type=str, required=True)
    parser.add_argument('--node-url', type=str, required=True)

    args = parser.parse_args()
    signer_key = Key.from_json_file(args.key_path)
    url_parse_result = urlparse(args.node_url)
    rpc_addr, rpc_port = url_parse_result.hostname, url_parse_result.port
    latest_block_hash = get_latest_block_hash(rpc_addr, rpc_port)
    nonce = get_nonce_for_key(signer_key, addr=rpc_addr, port=rpc_port)
    test_contract = load_test_contract()

    def deploy():
        deploy_mode = GlobalContractDeployMode()
        deploy_mode.enum = 'codeHash'
        deploy_mode.codeHash = ()
        deploy_tx = sign_deploy_global_contract_tx(signer_key, test_contract, deploy_mode,
                                            nonce+1, latest_block_hash)
        resp = json_rpc('broadcast_tx_async',
                        [base64.b64encode(deploy_tx).decode('utf8')],
                        rpc_addr, rpc_port)
        print(resp)
    
    def use():
        identifier = GlobalContractIdentifier()
        identifier.enum = "codeHash"
        identifier.codeHash = hashlib.sha256(test_contract).digest()
        use_tx = sign_use_global_contract_tx(signer_key, identifier, nonce + 1, latest_block_hash)
        resp = json_rpc('broadcast_tx_async',
                        [base64.b64encode(use_tx).decode('utf8')],
                        rpc_addr, rpc_port)
        print(resp)
    
    def call():
        call_tx = sign_function_call_tx(signer_key, signer_key.account_id, 'log_something',
                                [], 150 * TGAS, 1, nonce+1, latest_block_hash)
        resp = json_rpc('broadcast_tx_commit',
                        [base64.b64encode(call_tx).decode('utf8')],
                        rpc_addr, rpc_port)
        print(resp)
        print(resp['result']['receipts_outcome'][0]['outcome']['logs'][0])
    
    def create_account():
        key = Key.from_json_file("/Users/pugachag/Data/near_credentials/forknet_new_account.json")
        print(f"Create {key.account_id}")
        tx = sign_create_account_with_full_access_key_and_balance_tx(signer_key, key.account_id, key, 1 * NEAR_BASE, nonce + 1, latest_block_hash)
        resp = json_rpc('broadcast_tx_async',
                        [base64.b64encode(tx).decode('utf8')],
                        rpc_addr, rpc_port)
        print(resp)
    
    call()

