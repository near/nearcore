import base64
from concurrent.futures import ThreadPoolExecutor
import random
import string
import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from key import Key
from argparse import ArgumentParser, Action
from urllib.parse import urlparse
from utils import load_binary_file
from configured_logger import logger
from transaction import sign_deploy_contract_tx, sign_function_call_tx
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc, tx_result

if __name__ == '__main__':
    parser = ArgumentParser(
        description=
        'Deploy new contract')
    parser.add_argument('--key-path', type=str, required=True)
    parser.add_argument('--node-url', type=str, required=True)

    args = parser.parse_args()
    signer_key = Key.from_json_file(args.key_path)
    url_parse_result = urlparse(args.node_url)
    rpc_addr, rpc_port = url_parse_result.hostname, url_parse_result.port

    nonce = get_nonce_for_key(signer_key, addr=rpc_addr, port=rpc_port)

    while True:
        latest_block_hash = get_latest_block_hash(rpc_addr, rpc_port)
        contract_size = 100000
        contract_bytes = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(contract_size))
        contract = contract_bytes.encode() 
        nonce += 1
        signed_tx = sign_deploy_contract_tx(
            signer_key,
            contract,
            nonce,
            latest_block_hash,
        )
        resp = json_rpc('broadcast_tx_async',
                    [base64.b64encode(signed_tx).decode('utf8')],
                    rpc_addr, rpc_port)
        logger.info(f"Deployed new code: {resp}")

        for _ in range(5):
            latest_block_hash = get_latest_block_hash(rpc_addr, rpc_port)
            nonce += 1
            signed_tx = sign_function_call_tx(
                signer_key,
                "astro-stakers.poolv1.near",
                "doesn'tmatter",
                [],
                300000000000000,
                300000000000000,
                nonce,
                latest_block_hash,
            )
            resp = json_rpc('broadcast_tx_async',
                        [base64.b64encode(signed_tx).decode('utf8')],
                        rpc_addr, rpc_port)
            logger.info(f"Call sent: {resp}")
            #status = tx_result(resp["result"], "astro-stakers.poolv1.near", wait_for_success=True, addr=rpc_addr, port=rpc_port)
            #logger.info(f"Status: {status}")
            time.sleep(1.0)
