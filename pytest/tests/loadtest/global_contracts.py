import base64
from concurrent.futures import ThreadPoolExecutor
import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from key import Key
from argparse import ArgumentParser, Action
from urllib.parse import urlparse
from configured_logger import logger
from transaction import sign_function_call_tx, sign_deploy_global_contract_tx
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc
from messages.tx import GlobalContractDeployMode
from utils import load_test_contract

def create_sized_contract(size_mb: int) -> bytes:
    """minimal WASM contract of exactly size_mb megabytes."""
    size_bytes = size_mb * 1024 * 1024
    wasm_header = bytes([
        0x00, 0x61, 0x73, 0x6d,
        0x01, 0x00, 0x00, 0x00,
        0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
        0x03, 0x02, 0x01, 0x00,
        0x05, 0x03, 0x01, 0x00, 0x01,
        0x0b,
    ])
    
    data_length = size_bytes + 5
    data_length_bytes = []
    while data_length > 0:
        data_length_bytes.append(data_length & 0x7f)
        data_length >>= 7
    data_length_bytes = bytes(data_length_bytes)

    wasm = wasm_header + data_length_bytes + bytes([0x00, 0x41, 0x00, 0x0b]) + b'x' * size_bytes

    return wasm

# Real accounts from mainnet
shard_receiver_accounts = [
    "0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0.near",
    "aurora",
    "aurora-0.near",
    "game.hot.tg",
    "kkuuue2akv_1630967379.near",
    "tge-lockup.sweat",
]

if __name__ == '__main__':
    parser = ArgumentParser(
        description=
        'Generates constant stream of global contract deployment transactions')
    parser.add_argument('--key-path', type=str, required=True)
    parser.add_argument('--node-url', type=str, required=True)
    parser.add_argument('--shard-id', type=int, required=False)
    parser.add_argument('--contract-size-mb', type=int, default=1,
                      help='Size of the contract in megabytes')

    args = parser.parse_args()
    signer_key = Key.from_json_file(args.key_path)
    url_parse_result = urlparse(args.node_url)
    rpc_addr, rpc_port = url_parse_result.hostname, url_parse_result.port
    receivers = [shard_receiver_accounts[args.shard_id]
                ] if args.shard_id is not None else shard_receiver_accounts

    num_shards = len(receivers)
    executor = ThreadPoolExecutor(max_workers=num_shards)
    nonce = get_nonce_for_key(signer_key, addr=rpc_addr, port=rpc_port)

    code = create_sized_contract(args.contract_size_mb)
    deploy_mode = GlobalContractDeployMode()
    deploy_mode.enum = 'codeHash'
    deploy_mode.codeHash = ()
    
    while True:
        latest_block_hash = get_latest_block_hash(rpc_addr, rpc_port)

        def send_tx(i, receiver):
            # sleeping here to make best-effort ordering of transactions so nonces are valid

            start = time.time()
            time.sleep(i / 20)
            signed_tx = sign_deploy_global_contract_tx(
                signer_key,
                code,
                deploy_mode,
                nonce + i,
                latest_block_hash,
            )
            elapsed = time.time() - start
            logger.info(f"Time to sign tx: {elapsed:.2f} seconds")
            try:
                resp = json_rpc('broadcast_tx_async',
                                [base64.b64encode(signed_tx).decode('utf8')],
                                rpc_addr, rpc_port)
                logger.info(f"Transactions to {receiver} sent: {resp}")
            except Exception as ex:
                logger.error(f"Failed to send tx: {ex}")

        start = time.time()
        list(
            executor.map(lambda pr: send_tx(pr[0], pr[1]),
                         enumerate(receivers)))
        elapsed = time.time() - start
        logger.info(f"Sent global contract deployment transaction in {elapsed:.2f} seconds")
        # if elapsed < 1:
        #     time.sleep(1 - elapsed)
        if elapsed < 0.5:
            time.sleep(0.5 - elapsed)
        nonce += 1
