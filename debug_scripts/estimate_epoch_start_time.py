import requests
import time
import math
import argparse

def get_block(url, block_hash):
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "block",
        "params": {"block_id": block_hash} if block_hash else {"finality": "final"}
    }
    return requests.post(url, json=payload).json()['result']['header']

def ns_to_seconds(ns):
    return ns / 1e9

def format_time(seconds):
    return time.strftime("%H hours, %M minutes", time.gmtime(seconds))

def get_exponential_weighted_epoch_lengths(url, starting_block_hash, num_epochs, decay_rate=0.1):
    epoch_lengths, current_hash = [], starting_block_hash

    for i in range(num_epochs):
        block_data = get_block(url, current_hash)
        prev_block_data = get_block(url, block_data['next_epoch_id'])
        epoch_lengths.append(ns_to_seconds(int(block_data['timestamp']) - int(prev_block_data['timestamp'])))
        print(f"Epoch -{i+1}: {format_time(epoch_lengths[-1])}")
        current_hash = block_data['next_epoch_id']

    weights = [math.exp(-decay_rate * i) for i in range(num_epochs)]
    weighted_avg = sum(l * w for l, w in zip(epoch_lengths, weights)) / sum(weights)

    print(f"\nExponential weighted average epoch length: {format_time(weighted_avg)}")
    return epoch_lengths, weighted_avg

def predict_future_epochs(starting_epoch_timestamp, avg_epoch_length, num_future_epochs):
    future_epochs = []
    current_timestamp = ns_to_seconds(starting_epoch_timestamp)

    for i in range(1, num_future_epochs + 1):
        future_timestamp = current_timestamp + (i * avg_epoch_length)
        future_date = time.strftime('%Y-%m-%d %H:%M:%S %A', time.gmtime(future_timestamp))
        future_epochs.append(future_date)
        print(f"Predicted start of epoch {i}: {future_date}")

    return future_epochs

def main(args):
    latest_block = get_block(args.url, None)
    current_epoch_first_block = get_block(args.url, latest_block['next_epoch_id'])
    current_timestamp = int(current_epoch_first_block['timestamp'])

    epoch_lengths, exp_weighted_avg_epoch_length = get_exponential_weighted_epoch_lengths(
        args.url, latest_block['next_epoch_id'], args.num_past_epochs, args.decay_rate)

    predict_future_epochs(current_timestamp, exp_weighted_avg_epoch_length, args.num_future_epochs)

class SetURLFromChainID(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        urls = {
            'mainnet': 'https://archival-rpc.mainnet.near.org',
            'testnet': 'https://archival-rpc.testnet.near.org'
        }
        setattr(namespace, 'url', urls.get(values))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Approximate future epoch start dates for NEAR Protocol.")
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--url", help="The RPC URL to query.")
    group.add_argument("--chain_id", choices=['mainnet', 'testnet'], action=SetURLFromChainID, help="Sets the corresponding URL.")
    parser.add_argument("--num_past_epochs", type=int, default=4, help="Number of past epochs to analyze.")
    parser.add_argument("--decay_rate", type=float, default=0.1, help="Decay rate for exponential weighting.")
    parser.add_argument("--num_future_epochs", type=int, default=3, help="Number of future epochs to predict.")
    args = parser.parse_args()
    main(args)
