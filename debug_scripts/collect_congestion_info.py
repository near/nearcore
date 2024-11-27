#!/usr/bin/env python3

import argparse
import csv
import requests


def get_block_reference(block_id):
    if block_id:
        return {"block_id": block_id}
    else:
        return {"finality": "final"}


def get_chunk_reference(chunk_hash):
    return {"chunk_id": chunk_hash}


def get_block(url, block_id):
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "block",
        "params": get_block_reference(block_id)
    }

    try:
        response = requests.post(url, json=payload)
        return response.json()['result']
    except Exception as e:
        print(f"Failed to get block {block_id} from {url}. Error: {e}")
        return None


def get_congestion_level(url, chunk_hash):
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "EXPERIMENTAL_congestion_level",
        "params": get_chunk_reference(chunk_hash)
    }

    try:
        response = requests.post(url, json=payload)
        response = response.json()
        return response['result']["congestion_level"]
    except Exception as e:
        print(
            f"Failed to get congestion level for chunk {chunk_hash} from {url}. Response: {response} Error: {e}"
        )
        return None


class SetURLFromChainID(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        if values == 'mainnet':
            setattr(namespace, 'url', 'https://archival-rpc.mainnet.near.org')
        elif values == 'testnet':
            setattr(namespace, 'url', 'https://archival-rpc.testnet.near.org')


class ShardStats:

    def __init__(self):
        self.total = 0
        self.above_05 = 0

    def update(self, congestion_level):
        self.total += 1
        if congestion_level > 0.5:
            self.above_05 += 1

    def __str__(self):
        above_05_percent = 100 * self.above_05 / self.total if self.total else 0
        return f"Total: {self.total}, Congested: {self.above_05} Congested %: {above_05_percent}%"


def main(args):
    end_block = get_block(args.url, args.end_block_id)

    if args.start_block_id < 0:
        args.start_block_id = end_block["header"]["height"] + args.start_block_id
    start_block = get_block(args.url, args.start_block_id)

    start_height = start_block["header"]["height"]
    end_height = end_block["header"]["height"]

    shard_stats = {
        chunk["shard_id"]: ShardStats() for chunk in start_block["chunks"]
    }

    i = 1
    max_i = end_height - start_height
    with open(args.result_file, mode="w", newline="") as file:
        writer = csv.writer(file)

        # Write header
        writer.writerow(["block_height", "shard_id", "congestion_level"])

        for height in range(start_height, end_height):
            i += 1
            current_block = get_block(args.url, height)
            if not current_block:
                continue

            if not i % 10:
                print(f"progress {i: 8}/{max_i} height - {height}")

            for chunk in current_block["chunks"]:
                chunk_hash = chunk["chunk_hash"]
                shard_id = chunk["shard_id"]
                if args.shard_ids and str(shard_id) not in args.shard_ids:
                    continue
                congestion_level = get_congestion_level(args.url, chunk_hash)
                if congestion_level is None:
                    continue
                writer.writerow([height, shard_id, congestion_level])
                shard_stats[shard_id].update(congestion_level)

    print("Shard stats:")
    for shard_id, stats in shard_stats.items():
        print(f"Shard {shard_id}: {stats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect congestion data for a specific period.")
    # Create a mutually exclusive group for chain_id and url
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--url", help="RPC URL to query.")
    group.add_argument(
        "--chain_id",
        choices=['mainnet', 'testnet'],
        action=SetURLFromChainID,
        help="Chain ID ('mainnet' or 'testnet'). Sets the RPC URL.",
    )
    parser.add_argument(
        "--result_file",
        default="congestion_data.csv",
        help="Path to the output file (default: congestion_data.csv).",
    )
    parser.add_argument(
        "--start_block_id",
        type=int,
        help="Starting block ID for congestion data.",
        default=-1000,
    )
    parser.add_argument(
        "--end_block_id",
        type=int,
        default=0,
        help=
        "Ending block ID for congestion data (default: latest block at the invoke time).",
    )
    parser.add_argument(
        "--shard_ids",
        default=[],
        help="List of shard IDs to query. Empty for all shards.",
    )

    args = parser.parse_args()
    main(args)
