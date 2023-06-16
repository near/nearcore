#!/usr/bin/env python3
"""
Prober that is compatible with cloudprober.

The ProberSplit queries two nodes for blocks and chunks at random heights and
compares the results. The expectation is that the block and chunks at each
height will be identical even when fetched from two different nodes. It also
executes a contract view call on both nodes and compares the results.

The prober runs continuously for the duration specified in the command line
arguments. It runs at least one block and chunk check at a random height.

The intended goal of this prober is ensure that a legacy archival node and a
split storage archival node contain the same data.

Run like this:
./prober_split.py --chain-id testnet --split-url http://split.archival.node:3030 --duration-ms 20000

"""

import argparse
import datetime
import random
import sys
import subprocess
from datetime import datetime, timedelta

from prober_util import *


def check_genesis(legacy_url: str, split_url: str) -> int:
    legacy_genesis_height = get_genesis_height(legacy_url)
    split_genesis_height = get_genesis_height(split_url)

    if legacy_genesis_height != split_genesis_height:
        logger.error(
            "The genesis height is different. legacy: {}, split {}",
            legacy_genesis_height,
            split_genesis_height,
        )
        sys.exit(1)

    return legacy_genesis_height


def check_head(legacy_url: str, split_url: str, genesis_height: int) -> int:
    legacy_head_height = get_head(legacy_url)
    split_head_height = get_head(split_url)

    if legacy_head_height <= genesis_height:
        logger.error(
            '{} head must be higher than genesis. Got {} and {}',
            legacy_url,
            legacy_head_height,
            genesis_height,
        )
        sys.exit(1)

    if split_head_height <= genesis_height:
        logger.error(
            '{} head must be higher than genesis. Got {} and {}',
            split_url,
            split_head_height,
            genesis_height,
        )
        sys.exit(1)

    return min(legacy_head_height, split_head_height)


def check_blocks(legacy_url: str, split_url: str, height: int):
    logger.info(f"Checking blocks at height {height}.")

    legacy_block = get_block(height, legacy_url)
    split_block = get_block(height, split_url)

    if legacy_block != split_block:
        logger.error(
            f"Block check failed, the legacy block and the split block are different",
            f"\nlegacy block\n{pretty_print(legacy_block)}"
            f"\nsplit block\n{pretty_print(split_block)}")
        sys.exit(1)

    return legacy_block


def check_chunks(legacy_url: str, split_url: str, block):
    if block is None:
        return

    logger.info(f"Checking chunks.")
    for chunk in block['chunks']:
        legacy_chunk = get_chunk(chunk, legacy_url)
        split_chunk = get_chunk(chunk, split_url)

        if legacy_chunk != split_chunk:
            logger.error(
                f"Chunk check failed, the legacy chunk and the split chunk are different"
                f"\nlegacy chunk\n{pretty_print(legacy_chunk)}"
                f"\nsplit chunk\n{pretty_print(split_chunk)}")
            sys.exit(1)


def check_view_call(legacy_url, split_url):
    logger.info(f"Checking view call.")

    # This is the example contract function call from
    # https://docs.near.org/api/rpc/contracts#call-a-contract-function
    params = {
        "request_type": "call_function",
        "finality": "final",
        "account_id": "dev-1588039999690",
        "method_name": "get_num",
        "args_base64": "e30="
    }
    legacy_resp = json_rpc('query', params, legacy_url)
    split_resp = json_rpc('query', params, split_url)

    if legacy_resp['result']['result'] != split_resp['result']['result']:
        logger.error(
            f'View call check failed, the legacy response and the split response are different'
            f'\nlegacy response\n{legacy_resp}'
            f'\nsplit response\n{split_resp}')
        sys.exit(1)


# Query gcp for the archive nodes, pick a random one and return its url.
def get_random_legacy_url(chain_id):
    cmd = [
        'gcloud',
        'compute',
        'instances',
        'list',
    ]
    logger.info(" ".join(cmd))
    result = subprocess.run(cmd, text=True, capture_output=True)
    stdout = result.stdout
    lines = stdout.split('\n')
    pattern = f'{chain_id}-rpc-archive-public'
    lines = list(filter(lambda line: pattern in line, lines))
    line = random.choice(lines)
    tokens = line.split()
    external_ip = tokens[4]

    logger.info(f'Selected random legacy node - {external_ip}')
    return f'http://{external_ip}:3030'


def main():
    start_time = datetime.now()

    parser = argparse.ArgumentParser(
        description='Run a prober for split archival nodes')
    parser.add_argument('--chain-id', required=True, type=str)
    parser.add_argument('--split-url', required=True, type=str)
    parser.add_argument('--duration-ms', default=2000, type=int)
    parser.add_argument('--log-level', default="INFO")
    args = parser.parse_args()

    logger.setLevel(args.log_level)
    # log an empty line for cloudprober nice formatting
    logger.info('')
    logger.info('Running Prober Split')

    legacy_url = get_random_legacy_url(args.chain_id)
    split_url = args.split_url
    duration = timedelta(milliseconds=args.duration_ms)

    genesis_height = check_genesis(legacy_url, split_url)
    head = check_head(legacy_url, split_url, genesis_height)
    logger.info(f'The genesis height is {genesis_height}.')
    logger.info(f'The head height is {head}')

    check_view_call(legacy_url, split_url)

    # Verify multiple heights - optimization to allow the prober to verify
    # multiple heights in a single run.
    count = 0
    none_count = 0
    while True:
        # Pick a random number and then check the block and chunks at that height.
        height = random.randint(genesis_height, head)
        block = check_blocks(legacy_url, split_url, height)
        check_chunks(legacy_url, split_url, block)

        count += 1
        none_count += block is None

        current_time = datetime.now()
        current_duration = current_time - start_time
        if current_duration >= duration:
            break

        time.sleep(0.200)

    logger.info(
        f"Success. Validated {count} blocks. There were {none_count} missing blocks."
    )


if __name__ == '__main__':
    main()
