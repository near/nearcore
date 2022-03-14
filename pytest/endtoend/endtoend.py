#!/usr/bin/env python3
"""
End-to-end test for canary nodes.
Assuming each canary node has an account with positive balance.
Periodically (1 minute) each account sends itself 0 tokens. Some tokens get burned in the process.
Account balances get exported to prometheus and can be used to detect when transactions stop affecting the world, aka the chain of testnet or mainnet.
We observe effects on the chain using public RPC endpoints to ensure canaries are running a fork.

python3 endtoend/endtoend.py
    --ips <ip_node1,ip_node2>
    --accounts <account_node1,account_node2>
    --interval-sec 60
    --port 3030
    --public-key <public_key of test accounts>
    --private-key <private key of test account>
    --rpc-server-addr rpc.testnet.near.org
    --rpc-server-port 80
    --metrics-port 3040
"""
import argparse
import random
import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1] / 'lib'))

import account as account_mod
import key as key_mod
import mocknet_helpers
from concurrent.futures import ThreadPoolExecutor

from configured_logger import logger
import prometheus_client

balance_gauge = prometheus_client.Gauge(
    'near_e2e_account_balance',
    'Balance of the test accounts running an end-to-end test. Measured in yoctonear.',
    ['account'])


def do_ping(account, rpc_server):
    account_id = account.key.account_id
    base_block_hash = mocknet_helpers.get_latest_block_hash(addr=rpc_server[0],
                                                            port=rpc_server[1])
    balance = mocknet_helpers.retry_and_ignore_errors(
        lambda: mocknet_helpers.get_amount_yoctonear(
            account_id, addr=rpc_server[0], port=rpc_server[1]))
    if balance is not None:
        balance_gauge.labels(account=account_id).set(balance)
    logger.info(f'Sending 0 tokens from {account_id} to itself')
    tx_res = mocknet_helpers.retry_and_ignore_errors(
        lambda: account.send_transfer_tx(
            account_id, transfer_amount=0, base_block_hash=base_block_hash))
    logger.info(
        f'Sending 0 tokens from {account_id} to itself, tx result: {tx_res}')


def pinger(account, interval, rpc_server):
    account_id = account.key.account_id
    logger.info(f'pinger {account_id}')
    time.sleep(random.random() * interval)
    logger.info(f'pinger {account_id} woke up')
    while True:
        try:
            do_ping(account, rpc_server)
        except Exception as ex:
            logger.warning(f'Error pinging account {account_id}', exc_info=ex)
        time.sleep(interval)


if __name__ == '__main__':
    logger.info('Starting end-to-end test.')
    parser = argparse.ArgumentParser(description='Run an end-to-end test')
    parser.add_argument('--ips', required=True)
    parser.add_argument('--accounts', required=True)
    parser.add_argument('--interval-sec', type=float, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--public-key', required=True)
    parser.add_argument('--private-key', required=True)
    parser.add_argument('--rpc-server-addr', required=True)
    parser.add_argument('--rpc-server-port', required=True)
    parser.add_argument('--metrics-port', type=int, required=True)

    args = parser.parse_args()

    assert args.ips
    ips = args.ips.split(',')
    assert args.accounts
    account_ids = args.accounts.split(',')
    assert len(account_ids) == len(
        ips), 'List of test accounts must match the list of node IP addresses'
    interval_sec = args.interval_sec
    assert interval_sec > 1, 'Need at least 1 second between pings'
    port = args.port
    pk, sk = args.public_key, args.private_key
    rpc_server = (args.rpc_server_addr, args.rpc_server_port)

    keys = [key_mod.Key(account_id, pk, sk) for account_id in account_ids]
    base_block_hash = mocknet_helpers.get_latest_block_hash(addr=rpc_server[0],
                                                            port=rpc_server[1])
    accounts = []
    for ip, key in zip(ips, keys):
        nonce = mocknet_helpers.get_nonce_for_pk(key.account_id,
                                                 key.pk,
                                                 addr=rpc_server[0],
                                                 port=rpc_server[1])
        accounts.append(
            account_mod.Account(key,
                                nonce,
                                base_block_hash,
                                rpc_infos=[(ip, port)]))

    prometheus_client.start_http_server(args.metrics_port)

    with ThreadPoolExecutor() as executor:
        for account in accounts:
            executor.submit(pinger, account, interval_sec, rpc_server)
