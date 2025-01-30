import requests
import time
import math
import argparse
from datetime import datetime, timedelta
import pytz


# Function to get block data
def get_block(url, block_hash):
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "block",
    }

    payload["params"] = ({
        "block_id": block_hash
    } if block_hash is not None else {
        "finality": "final"
    })

    response = requests.post(url, json=payload)
    return response.json()["result"]["header"]


def ns_to_seconds(ns):
    return ns / 1e9


def format_time(seconds):
    # cspell:words gmtime
    return time.strftime("%H hours, %M minutes", time.gmtime(seconds))


# Function to fetch epoch lengths for the past n epochs and calculate the weighted average using exponential decay
def get_exponential_weighted_epoch_lengths(url,
                                           starting_block_hash,
                                           num_epochs,
                                           decay_rate=0.1):
    epoch_lengths = []
    current_hash = starting_block_hash

    for i in range(num_epochs):
        # Get the block data by hash
        block_data = get_block(url, current_hash)

        # Get the timestamp of this block (start of current epoch)
        current_timestamp = int(block_data["timestamp"])

        # Get the next epoch hash (last block hash of previous epoch.)
        previous_hash = block_data["next_epoch_id"]

        # Fetch the block data for start of previous epoch
        previous_block_data = get_block(url, previous_hash)
        previous_timestamp = int(previous_block_data["timestamp"])

        # Calculate the length of the epoch in nanoseconds
        epoch_length = current_timestamp - previous_timestamp
        epoch_length_seconds = ns_to_seconds(epoch_length)  # Convert to seconds
        epoch_lengths.append(epoch_length_seconds)

        print(f"Epoch -{i+1}: {format_time(epoch_length_seconds)}")

        # Move to the next epoch
        current_hash = previous_hash

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
        f"\nExponential weighted average epoch length: {format_time(exponential_weighted_average_epoch_length)}"
    )

    return epoch_lengths, exponential_weighted_average_epoch_length


def valid_timezone(timezone_str):
    try:
        return pytz.timezone(timezone_str)
    except pytz.UnknownTimeZoneError:
        raise argparse.ArgumentTypeError(f"Invalid timezone '{timezone_str}'")


# Function to approximate future epoch start dates
def predict_future_epochs(starting_epoch_timestamp, avg_epoch_length,
                          num_future_epochs, target_timezone):
    future_epochs = []
    current_timestamp = ns_to_seconds(
        starting_epoch_timestamp)  # Convert from nanoseconds to seconds

    for i in range(1, num_future_epochs + 1):
        # Add the average epoch length for each future epoch
        future_timestamp = current_timestamp + (i * avg_epoch_length)
        future_epochs.append(future_timestamp)

        # Convert timestamp to datetime in target timezone
        future_datetime = datetime.fromtimestamp(future_timestamp,
                                                 target_timezone)

        # Format date
        future_date = future_datetime.strftime("%Y-%m-%d %H:%M:%S %Z%z %A")
        print(f"Predicted start of epoch {i}: {future_date}")

    return future_epochs


def find_epoch_for_timestamp(future_epochs, voting_timestamp):
    for epoch_number, epoch_timestamp in enumerate(future_epochs):
        if voting_timestamp < epoch_timestamp:
            return epoch_number
    return len(future_epochs)


def find_best_voting_hour(voting_date_str, future_epochs):
    # Working hours during which there are available NEAR engineers
    WORKING_HOURS_START = 8  # 8:00 UTC
    WORKING_HOURS_END = 22  # 22:00 UTC

    valid_hours = []

    for hour in range(24):
        # Construct datetime for each hour of the voting date
        voting_datetime = datetime.strptime(
            f"{voting_date_str} {hour:02d}:00:00", "%Y-%m-%d %H:%M:%S")
        voting_datetime = pytz.utc.localize(voting_datetime)
        voting_timestamp = voting_datetime.timestamp()

        # Find the epoch T in which the voting date falls
        epoch_T = find_epoch_for_timestamp(future_epochs, voting_timestamp)
        if epoch_T <= 0:
            continue  # Voting date is before the first predicted epoch

        # Calculate when the protocol upgrade will happen (start of epoch T+2)
        protocol_upgrade_epoch_number = epoch_T + 2
        if protocol_upgrade_epoch_number > len(future_epochs):
            print(
                "Not enough future epochs predicted to determine all the protocol upgrade times."
            )
            break

        protocol_upgrade_timestamp = future_epochs[protocol_upgrade_epoch_number
                                                   - 1]
        protocol_upgrade_datetime = datetime.fromtimestamp(
            protocol_upgrade_timestamp)
        upgrade_hour_utc = protocol_upgrade_datetime.hour

        if WORKING_HOURS_START <= upgrade_hour_utc < WORKING_HOURS_END:
            valid_hours.append((hour, protocol_upgrade_epoch_number))

    if valid_hours:
        print(
            f"\nVoting hours on {voting_date_str} UTC that result in upgrade during working hours (UTC {WORKING_HOURS_START}:00-{WORKING_HOURS_END}:00):"
        )
        for hour, epoch in valid_hours:
            print(f"- {hour:02d}:00, Upgrade Epoch: {epoch}")
    else:
        print(
            "\nNo voting hours on the given date result in an upgrade during working hours."
        )


def valid_voting_datetime(s):
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid voting date format: '{s}'. Please use 'YYYY-MM-DD HH:MM:SS'."
        )


def find_protocol_upgrade_time(voting_date, future_epochs, target_timezone):
    voting_datetime = target_timezone.localize(voting_date)

    # Find the epoch T in which the voting date falls
    epoch_T = find_epoch_for_timestamp(future_epochs,
                                       voting_datetime.timestamp())
    if epoch_T <= 0:
        print("Error: Voting date is before the first predicted epoch.")
        return

    # Calculate when the protocol upgrade will happen (start of epoch T+2)
    protocol_upgrade_epoch_number = epoch_T + 2
    if protocol_upgrade_epoch_number > len(future_epochs):
        print(
            "Not enough future epochs predicted to determine the protocol upgrade time."
        )
        return
    protocol_upgrade_timestamp = future_epochs[protocol_upgrade_epoch_number -
                                               1]
    protocol_upgrade_datetime = datetime.fromtimestamp(
        protocol_upgrade_timestamp, tz=target_timezone)
    protocol_upgrade_formatted = protocol_upgrade_datetime.strftime(
        "%Y-%m-%d %H:%M:%S %Z%z %A")
    print(f"\nVoting date falls into epoch {epoch_T}.")
    print(
        f"Protocol upgrade will happen at the start of epoch {protocol_upgrade_epoch_number}: {protocol_upgrade_formatted}"
    )


# Main function to run the process
def main(args):
    latest_block = get_block(args.url, None)
    next_epoch_id = latest_block["next_epoch_id"]
    current_epoch_first_block = get_block(args.url, next_epoch_id)
    current_timestamp = int(current_epoch_first_block["timestamp"]
                           )  # Current epoch start timestamp in nanoseconds

    # Get epoch lengths and the exponential weighted average
    epoch_lengths, exponential_weighted_average_epoch_length = (
        get_exponential_weighted_epoch_lengths(args.url, next_epoch_id,
                                               args.num_past_epochs,
                                               args.decay_rate))

    # Predict future epoch start dates
    future_epochs = predict_future_epochs(
        current_timestamp,
        exponential_weighted_average_epoch_length,
        args.num_future_epochs,
        args.timezone,
    )

    if args.voting_date:
        find_protocol_upgrade_time(args.voting_date, future_epochs,
                                   args.timezone)
    elif args.voting_date_day:
        find_best_voting_hour(args.voting_date_day, future_epochs)


# Custom action to set the URL based on chain_id
class SetURLFromChainID(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        if values == "mainnet":
            setattr(namespace, "url", "https://archival-rpc.mainnet.near.org")
        elif values == "testnet":
            setattr(namespace, "url", "https://archival-rpc.testnet.near.org")


# Set up command-line argument parsing
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Approximate future epoch start dates for NEAR Protocol.")
    # Create a mutually exclusive group for chain_id and url
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--url", help="The RPC URL to query.")
    group.add_argument(
        "--chain_id",
        choices=["mainnet", "testnet"],
        action=SetURLFromChainID,
        help=
        "The chain ID (either 'mainnet' or 'testnet'). Sets the corresponding URL.",
    )

    parser.add_argument(
        "--num_past_epochs",
        type=int,
        default=4,
        help="Number of past epochs to analyze.",
    )
    parser.add_argument(
        "--decay_rate",
        type=float,
        default=0.1,
        help="Decay rate for exponential weighting.",
    )
    parser.add_argument(
        "--num_future_epochs",
        type=int,
        default=10,
        help="Number of future epochs to predict.",
    )
    parser.add_argument(
        "--timezone",
        type=valid_timezone,
        default="UTC",
        help="Time zone to display times in (e.g., 'America/New_York').",
    )
    # Voting date arguments
    voting_group = parser.add_mutually_exclusive_group()
    voting_group.add_argument(
        "--voting_date",
        type=valid_voting_datetime,
        help="Voting date in 'YYYY-MM-DD HH:MM:SS' format.",
    )
    voting_group.add_argument(
        "--voting_date_day",
        help=
        "Voting date (day) in 'YYYY-MM-DD' format to find voting hours resulting in upgrade during working hours.",
    )

    args = parser.parse_args()
    main(args)
