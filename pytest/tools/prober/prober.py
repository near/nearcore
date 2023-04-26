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
import pathlib
import random
import sys

from prober_util import *

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))


def main():
    # log an empty line for cloudprober nice formatting
    logger.info('')
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

        # Some blocks are really missing and there is no way to know if they are
        # missing because the node doesn't have history, or because the block
        # was skipped.
        if random_height == genesis_height:
            logger.error(
                f'Genesis block is missing. This is impossible {random_height}')
            sys.exit(1)

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
    timestamp = block["header"]["timestamp"] // 10**9
    timestamp = datetime.datetime.fromtimestamp(timestamp)
    logger.info(f'Block {random_height} timestamp {timestamp}')
    if num_chunks > 0:
        chunk = random.choice(block['chunks'])
        get_chunk(chunk, url)

    logger.info('Success.')


if __name__ == '__main__':
    main()
