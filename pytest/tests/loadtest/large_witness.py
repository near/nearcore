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
from transaction import sign_function_call_tx
from mocknet_helpers import get_latest_block_hash, get_nonce_for_key, json_rpc

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
        'Generates constant stream of transactions to trigger large witness')
    parser.add_argument('--key-path', type=str, required=True)
    parser.add_argument('--node-url', type=str, required=True)
    parser.add_argument('--witness-size-mbs', type=int, required=True)
    parser.add_argument('--shard-id', type=int, required=False)

    args = parser.parse_args()
    signer_key = Key.from_json_file(args.key_path)
    url_parse_result = urlparse(args.node_url)
    rpc_addr, rpc_port = url_parse_result.hostname, url_parse_result.port
    receivers = [shard_receiver_accounts[args.shard_id]
                ] if args.shard_id is not None else shard_receiver_accounts

    num_shards = len(receivers)
    executor = ThreadPoolExecutor(max_workers=num_shards)
    nonce = get_nonce_for_key(signer_key, addr=rpc_addr, port=rpc_port)
    while True:
        latest_block_hash = get_latest_block_hash(rpc_addr, rpc_port)

        def send_tx(i, receiver):
            # sleeping here to make best-effort ordering of transactions so nonces are valid
            time.sleep(i / 20)
            signed_tx = sign_function_call_tx(
                signer_key,
                receiver,
                f"internal_record_storage_garbage_{args.witness_size_mbs}",
                [],
                300000000000000,
                300000000000000,
                nonce + i,
                latest_block_hash,
            )
            try:
                resp = json_rpc('broadcast_tx_async',
                                [base64.b64encode(signed_tx).decode('utf8')],
                                rpc_addr, rpc_port)
                logger.debug(f"Transactions to {receiver} sent: {resp}")
            except Exception as ex:
                logger.error(f"Failed to send trx: {ex}")

        start = time.time()
        list(
            executor.map(lambda pr: send_tx(pr[0], pr[1]),
                         enumerate(receivers)))
        elapsed = time.time() - start
        logger.info(f"Sent {num_shards} transactions in {elapsed:.2f} seconds")
        if elapsed < 0.5:
            time.sleep(0.5 - elapsed)
        nonce += num_shards
