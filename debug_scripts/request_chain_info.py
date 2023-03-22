#!/usr/bin/env python3

import requests
import json
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='This is a script to request for blockchain info')
    parser.add_argument('--chain',
                        choices=['mainnet', 'testnet', 'betanet'],
                        required=True)
    parser.add_argument('--archive',
                        action='store_true',
                        help='whether to request from archival nodes')
    parser.add_argument('--method',
                        choices=['block', 'chunk'],
                        required=True,
                        help='type of request')
    parser.add_argument('--block_id',
                        type=str,
                        help='block id, can be either block height or hash')
    parser.add_argument('--shard_id', type=int, help='shard id for the chunk')
    parser.add_argument('--chunk_id', type=str, help='chunk hash')
    parser.add_argument('--result_key',
                        type=str,
                        nargs='*',
                        help='filter results by these keys')
    args = parser.parse_args()

    url = 'https://{}.{}.near.org'.format(
        'archival-rpc' if args.archive else 'rpc', args.chain)

    def get_block_id(block_id):
        if block_id.isnumeric():
            return int(block_id)
        return block_id

    if args.method == 'block':
        if args.block_id is not None:
            params = {'block_id': get_block_id(args.block_id)}
        else:
            params = {'finality': 'final'}
    elif args.method == 'chunk':
        if args.shard_id is not None:
            assert args.block_id is not None
            params = {
                'shard_id': args.shard_id,
                'block_id': get_block_id(args.block_id)
            }
        elif args.chunk_id is not None:
            params = {'chunk_id': args.chunk_id}
    else:
        assert False

    payload = {
        'jsonrpc': '2.0',
        'id': 'dontcare',
        'method': args.method,
        'params': params
    }

    response = requests.post(url, json=payload)
    result = response.json()['result']
    if args.result_key is not None:
        for key in args.result_key:
            result = result[key]
    print(json.dumps(result, indent=4))
