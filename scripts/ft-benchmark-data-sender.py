import requests
from prometheus_client.parser import text_string_to_metric_families
from datetime import datetime
from time import sleep
import numpy as np
import subprocess
from os import chdir
import os
import json
import tempfile
import argparse

# TPS polling interval in seconds
POLL_INTERVAL = 30

# Maximum failed_tx / total_tx ratio we accept
EPS = 0.001


# Returns the total number of transactions processed by the network at the moment
def calculate_processed_transactions() -> int:
    interested_metrics = [
        "near_transaction_processed_successfully_total",
        "near_transaction_processed_total",
    ]
    metrics_dict = {
        "near_transaction_processed_successfully_total": None,
        "near_transaction_processed_total": None,
    }
    url = "http://127.0.0.1:3030/metrics"
    response = requests.get(url)
    if response.status_code != 200:
        raise f"Failed to retrieve TPS data. HTTP Status code: {response.status_code}"
    metrics_data = response.text
    for family in text_string_to_metric_families(metrics_data):
        for sample in family.samples:
            if sample.name in interested_metrics:
                metrics_dict[sample.name] = sample.value
    if (metrics_dict["near_transaction_processed_total"] -
            metrics_dict["near_transaction_processed_successfully_total"]
       ) / metrics_dict["near_transaction_processed_total"] > EPS:
        raise "Too much failed transactions"
    return metrics_dict["near_transaction_processed_successfully_total"]


# Returns the number of shards in the network
def calculate_shards() -> int:
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "EXPERIMENTAL_protocol_config",
        "params": {
            "finality": "final"
        },
    }
    url = "http://127.0.0.1:3030"
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise f"Failed to retrieve shards amount. HTTP Status code: {response.status_code}"
    return len(response.json()["result"]["shard_layout"])


# Returns a tuple with the commit hash and the commit timestamp
def get_commit() -> tuple[str, datetime]:
    payload = {
        "jsonrpc": "2.0",
        "id": "dontcare",
        "method": "status",
        "params": []
    }
    local_url = "http://127.0.0.1:3030"
    response = requests.post(local_url, json=payload)
    if response.status_code != 200:
        raise f"Failed to retrieve commit hash. HTTP Status code: {response.status_code}"
    version = response.json()["result"]["version"]["build"]
    short_commit = version.split("-")[2][1:]
    github_url = f"https://api.github.com/repos/near/nearcore/commits/{short_commit}"
    response = requests.get(github_url)
    commit_data = response.json()
    full_commit_hash = commit_data["sha"]
    commit_timestamp_str = commit_data["commit"]["author"]["date"]
    commit_timestamp = datetime.strptime(commit_timestamp_str,
                                         "%Y-%m-%dT%H:%M:%SZ")
    return (full_commit_hash, commit_timestamp)


def commit_to_db(data: dict) -> None:
    print(data)
    chdir(os.path.expanduser("~/nearcore/benchmarks/continous/db/tool"))
    with tempfile.NamedTemporaryFile(mode="w", encoding='utf-8') as fp:
        json.dump(data, fp)
        fp.flush()
        os.fsync(fp.fileno())
        subprocess.run(f"cargo run -p cli -- insert-ft-transfer {fp.name}",
                       shell=True)
        fp.close()


# TODO: send signal to this process if ft-benchmark.sh decided to switch neard to another commit.
# add handling of this signal to this script
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=
        "Collect data from local prometheus and send to ft-benchmark db.")
    parser.add_argument('--duration',
                        type=int,
                        required=True,
                        help='Duration of experiment in seconds')
    parser.add_argument('--users',
                        type=int,
                        required=True,
                        help='Number of users')
    args = parser.parse_args()
    DURATION = args.duration / 3600

    state_size = (int(
        subprocess.check_output(["du", "-s", "~/.near/localnet/node0/data"],
                                stderr=subprocess.PIPE,
                                shell=True).decode("utf-8").split()[0]) * 1024)
    processed_transactions = []
    time_begin = datetime.now()
    while True:
        print(
            f"Data sender loop. Time elapsed: {(datetime.now() - time_begin).seconds} seconds"
        )
        if (datetime.now() - time_begin).seconds / 3600 > DURATION:
            break
        processed_transactions.append(calculate_processed_transactions())
        sleep(POLL_INTERVAL)
    processed_transactions_deltas = np.diff(processed_transactions)
    processed_transactions_deltas = np.array(
        list(map(lambda x: x / POLL_INTERVAL, processed_transactions_deltas)))
    average_tps = np.mean(processed_transactions_deltas)
    variance_tps = np.var(processed_transactions_deltas)
    # TODO: will be good to have all "probably should be filled by terraform" as command line arguments
    # TODO: add start_time and end_time instead of time to db schema
    commit_hash, commit_time = get_commit()
    response = {
        "time": time_begin.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "git_commit_hash": commit_hash,
        "git_commit_time": commit_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "num_nodes": 1,  # TODO: probably should be filled by terraform
        "node_hardware": ["n2d-standard-8"
                         ],  # TODO: probably should be filled by terraform
        "num_traffic_gen_machines":
            0,  # TODO: probably should be filled by terraform
        "disjoint_workloads":
            False,  # TODO: probably should be filled by terraform
        "num_shards": calculate_shards(),
        "num_unique_users": args.users,
        "size_state_bytes": state_size,
        "tps": int(average_tps),
        "total_transactions": int(processed_transactions[-1]),
    }
    commit_to_db(response)
