import argparse
import csv
import requests
import time

from dataclasses import dataclass


def get_block_with_retries(url, block_id, retries=3, delay=2):
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "block",
    }

    payload["params"] = {
        "block_id": block_id
    } if block_id else {
        "finality": "final"
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, json=payload)
            return response.json()['result']
        except Exception as e:
            if attempt < retries - 1:
                print(
                    f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
            else:
                print(f"All {retries} attempts failed.")
                raise e


class SetURLFromChainID(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        if values == 'mainnet':
            setattr(namespace, 'url', 'https://archival-rpc.mainnet.near.org')
        elif values == 'testnet':
            setattr(namespace, 'url', 'https://archival-rpc.testnet.near.org')


@dataclass(frozen=True)
class ShardCongestionInfo:
    allowed_shard: int
    buffered_receipts_gas: int
    delayed_receipts_gas: int
    receipt_bytes: int


@dataclass(frozen=True)
class CongestionData:
    block_id: int
    congestion_info: [ShardCongestionInfo]


def main(args):
    start_block = get_block_with_retries(args.url, args.start_block_id)
    end_block = get_block_with_retries(args.url, args.end_block_id)
    current_block = end_block

    with open(args.result_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow([
            "block_id", "allowed_shard", "buffered_receipts_gas",
            "delayed_receipts_gas", "receipt_bytes"
        ])

        while current_block["header"]["height"] >= start_block["header"][
                "height"]:
            if not current_block["header"]["height"] % 10:
                print(current_block["header"]["height"])
            for chunk in current_block["chunks"]:
                shard_info = ShardCongestionInfo(**chunk["congestion_info"])
                if args.shard_ids and shard_info.allowed_shard not in args.shard_ids:
                    continue
                writer.writerow([
                    current_block["header"]["height"], shard_info.allowed_shard,
                    shard_info.buffered_receipts_gas,
                    shard_info.delayed_receipts_gas, shard_info.receipt_bytes
                ])
            current_block = get_block_with_retries(
                args.url, current_block["header"]["prev_height"])


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
        help="Chain ID ('mainnet' or 'testnet'). Sets the RPC URL.")
    parser.add_argument(
        "--result_file",
        default="congestion_data.csv",
        help="Path to the output file (default: congestion_data.csv).")
    parser.add_argument("--start_block_id",
                        type=int,
                        help="Starting block ID for congestion data.")
    parser.add_argument(
        "--end_block_id",
        type=int,
        default=0,
        help=
        "Ending block ID for congestion data (default: latest block at the invoke time)."
    )
    parser.add_argument(
        "--shard_ids",
        default=[],
        help="List of shard IDs to query. Empty for all shards.")

    args = parser.parse_args()
    main(args)
