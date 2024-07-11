import base64
import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from key import Key
from argparse import ArgumentParser, Action
from urllib.parse import urlparse
from configured_logger import logger
from transaction import sign_function_call_tx
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc

if __name__ == '__main__':
    parser = ArgumentParser(
        description=
        'Generates constant stream of transactions to trigger large witness')
    parser.add_argument('--key-path', type=str, required=True)
    parser.add_argument('--node-url', type=str, required=True)
    parser.add_argument('--witness-size-mbs', type=int, required=True)

    args = parser.parse_args()
    signer_key = Key.from_json_file(args.key_path)

    url_parse_result = urlparse(args.node_url)
    rpc_addr, rpc_port = url_parse_result.hostname, url_parse_result.port
    nonce = get_nonce_for_key(signer_key, addr=rpc_addr, port=rpc_port)
    while True:
        latest_block_hash = get_latest_block_hash(rpc_addr, rpc_port)
        nonce += 1
        signed_tx = sign_function_call_tx(
            signer_key,
            signer_key.account_id,
            f"internal_record_storage_garbage_{args.witness_size_mbs}",
            [],
            300000000000000,
            300000000000000,
            nonce,
            latest_block_hash,
        )
        resp = json_rpc('broadcast_tx_async',
                        [base64.b64encode(signed_tx).decode('utf8')], rpc_addr,
                        rpc_port)
        logger.info(f"Transactions sent: {resp}")
        time.sleep(0.5)
