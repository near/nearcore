#!/usr/bin/env python3
import json
import time
import base58
import requests
from configured_logger import logger

LOCAL_ADDR = '127.0.0.1'
RPC_PORT = '3030'


def get_status():
    r = requests.get(f'http://{LOCAL_ADDR}:{RPC_PORT}/status', timeout=10)
    r.raise_for_status()
    return r.json()


def json_rpc(method, params):
    j = {'method': method, 'params': params, 'id': 'dontcare', 'jsonrpc': '2.0'}
    r = requests.post(f'http://{LOCAL_ADDR}:{RPC_PORT}', json=j, timeout=10)
    return r.json()


def get_nonce_for_pk(account_id, pk, finality='optimistic'):
    access_keys = json_rpc(
        'query', {
            'request_type': 'view_access_key_list',
            'account_id': account_id,
            'finality': finality
        })
    logger.info(f'get_nonce_for_pk {account_id}')
    assert access_keys['result']['keys'], account_id
    for k in access_keys['result']['keys']:
        if k['public_key'] == pk:
            return k['access_key']['nonce']


def get_latest_block_hash():
    last_block_hash = get_status()['sync_info']['latest_block_hash']
    return base58.b58decode(last_block_hash.encode('utf-8'))


def throttle_txns(send_txns, total_tx_sent, elapsed_time, max_tps_per_node,
                  node_account, test_accounts):
    start_time = time.monotonic()
    send_txns(node_account, test_accounts, max_tps_per_node)
    duration = time.monotonic() - start_time
    total_tx_sent += len(test_accounts)

    excess_transactions = total_tx_sent - (max_tps_per_node *
                                           (elapsed_time + duration))
    if excess_transactions > 0:
        delay = excess_transactions / max_tps_per_node
        logger.info(f'Sleeping for {delay} seconds to throttle transactions')
        time.sleep(delay)

    return total_tx_sent


def retry_and_ignore_errors(f):
    for attempt in range(3):
        try:
            return f()
        except Exception as e:
            time.sleep(0.1 * 2**attempt)
    return None


def wait_at_least_one_block():
    status = get_status()
    start_height = status['sync_info']['latest_block_height']
    timeout_sec = 5
    started = time.monotonic()
    while time.monotonic() - started < timeout_sec:
        status = get_status()
        height = status['sync_info']['latest_block_height']
        if height > start_height:
            break
        time.sleep(1.0)
