import requests
import time
import math
import argparse


# Function to get block data by block hash
def get_block_by_hash(url, block_hash):
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "block",
        "params": {
            "block_id": block_hash
        }
    }
    response = requests.post(url, json=payload)
    return response.json()['result']['header']


# Function to fetch epoch lengths for the past n epochs and calculate the weighted average using exponential decay
def get_exponential_weighted_epoch_lengths(url,
                                           starting_epoch_hash,
                                           num_epochs,
                                           decay_rate=0.1):
    epoch_lengths = []
    current_hash = starting_epoch_hash

    for i in range(num_epochs):
        # Get the block data by hash
        block_data = get_block_by_hash(url, current_hash)

        # Get the timestamp of this block (start of current epoch)
        current_timestamp = int(block_data['timestamp'])

        # Get the next epoch hash (start block hash of current epoch)
        next_epoch_hash = block_data['next_epoch_id']

        # Fetch the block data for start of previous epoch
        next_block_data = get_block_by_hash(url, next_epoch_hash)
        next_timestamp = int(next_block_data['timestamp'])

        # Calculate the length of the epoch in nanoseconds
        epoch_length = current_timestamp - next_timestamp
        epoch_length_seconds = epoch_length / 1e9  # Convert to seconds
        epoch_lengths.append(epoch_length_seconds)

        print(f"Epoch {i+1}: {epoch_length_seconds} seconds")

        # Move to the next epoch
        current_hash = next_epoch_hash

    # Apply exponential decay weights: weight = e^(-lambda * i), where i is the epoch index and lambda is the decay rate
    weighted_sum = 0
    total_weight = 0
    for i in range(num_epochs):
        weight = math.exp(-decay_rate * i)
        weighted_sum += epoch_lengths[i] * weight
        total_weight += weight

    # Calculate the weighted average using exponential decay
    exponential_weighted_average_epoch_length = weighted_sum / total_weight

    print(
        f"\nExponential weighted average epoch length: {exponential_weighted_average_epoch_length} seconds"
    )

    return epoch_lengths, exponential_weighted_average_epoch_length


# Function to approximate future epoch start dates
def predict_future_epochs(starting_epoch_timestamp, avg_epoch_length,
                          num_future_epochs):
    future_epochs = []
    current_timestamp = starting_epoch_timestamp / 1e9  # Convert from nanoseconds to seconds

    for i in range(1, num_future_epochs + 1):
        # Add the average epoch length for each future epoch
        future_timestamp = current_timestamp + (i * avg_epoch_length)

        # Convert to human-readable format
        future_date = time.strftime('%Y-%m-%d %H:%M:%S %A',
                                    time.gmtime(future_timestamp))
        future_epochs.append(future_date)

        print(f"Predicted start of epoch {i}: {future_date}")

    return future_epochs


# First, we get the latest block and save the next_epoch_id
def get_latest_block(url):
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "block",
        "params": {
            "finality": "final"
        }
    }
    response = requests.post(url, json=payload).json()
    return response['result']['header']


# Main function to run the process
def main(args):
    latest_block = get_latest_block(args.url)
    next_epoch_id = latest_block['next_epoch_id']
    current_epoch_first_block = get_block_by_hash(args.url, next_epoch_id)
    current_timestamp = int(current_epoch_first_block['timestamp']
                           )  # Current epoch start timestamp in nanoseconds

    # Get epoch lengths and the exponential weighted average
    epoch_lengths, exponential_weighted_average_epoch_length = get_exponential_weighted_epoch_lengths(
        args.url, next_epoch_id, args.num_epochs, args.decay_rate)

    # Predict future epoch start dates
    predict_future_epochs(current_timestamp,
                          exponential_weighted_average_epoch_length,
                          args.num_future_epochs)


# Set up command-line argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Approximate future epoch start dates for NEAR Protocol.")
    parser.add_argument("--url", required=True, help="The RPC URL to query.")
    parser.add_argument("--num_epochs",
                        type=int,
                        default=4,
                        help="Number of past epochs to analyze.")
    parser.add_argument("--decay_rate",
                        type=float,
                        default=0.1,
                        help="Decay rate for exponential weighting.")
    parser.add_argument("--num_future_epochs",
                        type=int,
                        default=3,
                        help="Number of future epochs to predict.")

    args = parser.parse_args()
    main(args)
