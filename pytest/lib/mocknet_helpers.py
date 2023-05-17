#!/usr/bin/env python3
import time
import base58
import requests
from configured_logger import logger
from key import Key

LOCAL_ADDR = '127.0.0.1'
RPC_PORT = '3030'


def get_status(addr=LOCAL_ADDR, port=RPC_PORT):
    r = requests.get(f'http://{addr}:{port}/status', timeout=10)
    r.raise_for_status()
    return r.json()


def json_rpc(method, params, addr=LOCAL_ADDR, port=RPC_PORT):
    j = {'method': method, 'params': params, 'id': 'dontcare', 'jsonrpc': '2.0'}
    r = requests.post(f'http://{addr}:{port}', json=j, timeout=10)
    return r.json()


def get_nonce_for_key(key: Key, **kwargs) -> int:
    return get_nonce_for_pk(key.account_id, key.pk, **kwargs)


def get_nonce_for_pk(account_id,
                     pk,
                     finality='optimistic',
                     addr=LOCAL_ADDR,
                     port=RPC_PORT,
                     logger=logger):
    access_keys = json_rpc(
        'query',
        {
            'request_type': 'view_access_key_list',
            'account_id': account_id,
            'finality': finality
        },
        addr=addr,
        port=port,
    )
    logger.info(f'get_nonce_for_pk {account_id}')
    logger.info(access_keys)
    if not access_keys['result']['keys']:
        raise KeyError(account_id)

    nonce = next((key['access_key']['nonce']
                  for key in access_keys['result']['keys']
                  if key['public_key'] == pk), None)
    if nonce is None:
        raise KeyError(f'Nonce for {account_id} {pk} not found')
    return nonce


def get_latest_block_hash(addr=LOCAL_ADDR, port=RPC_PORT):
    last_block_hash = get_status(
        addr=addr,
        port=port,
    )['sync_info']['latest_block_hash']
    return base58.b58decode(last_block_hash.encode('utf-8'))


def throttle_txns(send_txns, total_tx_sent, elapsed_time, test_state):
    start_time = time.monotonic()
    send_txns(test_state)
    duration = time.monotonic() - start_time
    total_tx_sent += test_state.num_test_accounts()

    excess_transactions = total_tx_sent - (test_state.max_tps_per_node *
                                           (elapsed_time + duration))
    if excess_transactions > 0:
        delay = excess_transactions / test_state.max_tps_per_node
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


def get_amount_yoctonear(account_id, addr=LOCAL_ADDR, port=RPC_PORT):
    j = json_rpc('query', {
        'request_type': 'view_account',
        'finality': 'optimistic',
        'account_id': account_id
    },
                 addr=addr,
                 port=port)
    return int(j.get('result', {}).get('amount', 0))


# Returns the transaction result for the given txn_id.
# Might return None - if transaction is not present and wait_for_success is false.
def tx_result(txn_id, account_id, wait_for_success=False, **kwargs):
    while True:
        status = json_rpc("EXPERIMENTAL_tx_status", [txn_id, account_id],
                          **kwargs)
        if 'error' in status:
            print("tx error: tx not ready yet")
            if not wait_for_success:
                return None
            time.sleep(3)
        else:
            return status['result']
