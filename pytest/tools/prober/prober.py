#!/usr/bin/env python3
"""
Prober that is compatible with cloudprober.
Prober verifies that a random block since the genesis can be retrieved from a node.
Makes 3 separate RPC requests: Get genesis height, get head, get a block.
If the block contains chunks, then make one more RPC request to get a chunk.

Run like this:
./prober --url http://my.test.net.node:3030

"""

import argparse
import datetime
import json
import pathlib
import random
import requests
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import configured_logger

logger = configured_logger.new_logger("stderr", stderr=True)


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
        print(
            f'prober_request_latency_ms{{method="{method}",url="{url}"}} {latency_ms:.2f}'
        )
        result = json.loads(r.content)
        return result
    except Exception as e:
        logger.error(f'Query failed: {e}')
        sys.exit(1)


def get_genesis_height(url):
    try:
        genesis_config = json_rpc('EXPERIMENTAL_genesis_config', None, url)
        genesis_height = genesis_config['result']['genesis_height']
        logger.info(f'Got genesis_height {genesis_height}')
        return genesis_height
    except Exception as e:
        logger.error(f'get_genesis_height() failed: {e}')
        sys.exit(1)


def get_head(url):
    try:
        status = json_rpc('status', None, url)
        head = status['result']['sync_info']['latest_block_height']
        logger.info(f'Got latest_block_height {head}')
        return head
    except Exception as e:
        logger.error(f'get_head() failed: {e}')
        sys.exit(1)


def get_block(height, url):
    try:
        block = json_rpc('block', {'block_id': height}, url)
        block = block['result']
        logger.info(f'Got block at height {height}')
        return block
    except Exception as e:
        logger.error(f'get_block({height}) failed: {e}')
        return None


def get_chunk(chunk, url):
    try:
        shard_id = chunk['shard_id']
        chunk_hash = chunk['chunk_hash']
        chunk = json_rpc('chunk', {'chunk_id': chunk_hash}, url)
        chunk = chunk['result']
        logger.info(f'Got chunk {chunk_hash} for shard {shard_id}')
        return chunk
    except Exception as e:
        logger.error(f'get_chunk({chunk_hash}) failed: {e}')
        sys.exit(1)


def main():
    logger.info('Running Prober')
    parser = argparse.ArgumentParser(description='Run a prober')
    parser.add_argument('--url', required=True)
    args = parser.parse_args()

    url = args.url

    # Determine the genesis height and the head height.
    genesis_height = get_genesis_height(url)
    head = get_head(url)
    if head < genesis_height:
        logger.error(
            f'head must be higher than genesis. Got {head} and {genesis_height}'
        )
        sys.exit(1)

    # Pick a random number and then try to lookup a block at that height.
    random_height = random.randint(genesis_height, head)
    attempt = 0
    while True:
        block = get_block(random_height, url)
        if block:
            break

        # Some blocks are really missing and there is no way to miss if they are
        # missing because the node doesn't have history, or because the block
        # was skipped.
        if random_height == genesis_height:
            logger.error(
                f'Genesis block is missing. This is impossible {random_height}')

        random_height -= 1
        attempt += 1
        # Limit the number of attempts.
        if attempt > 10:
            logger.error(
                f'{attempt} consecutive blocks are missing. This is improbable. From {random_height + 1} to {random_height + attempt}'
            )
            sys.exit(1)

    # Lookup a chunk to make sure the node contains it.
    num_chunks = len(block['chunks'])
    logger.info(f'Block {random_height} contains {num_chunks} chunks')
    logger.info(
        f'Block {random_height} timestamp {datetime.datetime.fromtimestamp(block["header"]["timestamp"]//10**9)}'
    )
    if num_chunks > 0:
        chunk = random.choice(block['chunks'])
        get_chunk(chunk, url)


if __name__ == '__main__':
    main()
