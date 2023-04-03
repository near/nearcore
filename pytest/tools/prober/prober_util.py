#!/usr/bin/env python3

import sys
import json
import time
import pathlib
import requests
import json

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import configured_logger

logger = configured_logger.new_logger("stderr", stderr=True)


def pretty_print(value) -> str:
    return json.dumps(value, indent=2)


def json_rpc(method, params, url):
    try:
        j = {
            'method': method,
            'params': params,
            'id': 'dontcare',
            'jsonrpc': '2.0'
        }
        start_time = time.time()
        r = requests.post(url, json=j, timeout=5)
        latency_ms = (time.time() - start_time)
        logger.debug(
            f'prober_request_latency_ms{{method="{method}",url="{url}"}} {latency_ms:.2f}'
        )
        result = json.loads(r.content)
        return result
    except Exception as e:
        logger.error(f'json_rpc({method}, {url}) query failed: {e}')
        sys.exit(1)


def get_genesis_height(url):
    try:
        genesis_config = json_rpc('EXPERIMENTAL_genesis_config', None, url)
        genesis_height = genesis_config['result']['genesis_height']
        logger.debug(f'Got genesis_height {genesis_height}')
        return genesis_height
    except Exception as e:
        logger.error(f'get_genesis_height({url}) failed: {e}')
        sys.exit(1)


def get_head(url):
    try:
        status = json_rpc('status', None, url)
        head = status['result']['sync_info']['latest_block_height']
        logger.debug(f'Got latest_block_height {head}')
        return head
    except Exception as e:
        logger.error(f'get_head({url}) failed: {e}')
        sys.exit(1)


def get_block(height, url):
    try:
        block = json_rpc('block', {'block_id': height}, url)
        if 'error' in block:
            raise Exception(block['error'])
        block = block['result']
        logger.debug(f'Got block at height {height}')
        return block
    except Exception as e:
        # This is typically fine as there may be gaps in the chain.
        logger.error(f'get_block({height}, {url}) failed: {e}')
        return None


def get_chunk(chunk, url):
    try:
        shard_id = chunk['shard_id']
        chunk_hash = chunk['chunk_hash']
        chunk = json_rpc('chunk', {'chunk_id': chunk_hash}, url)
        chunk = chunk['result']

        logger.debug(f'Got chunk {chunk_hash} for shard {shard_id}')
        return chunk
    except Exception as e:
        logger.error(f'get_chunk({chunk_hash}, {url}) failed: {e}')
        sys.exit(1)
