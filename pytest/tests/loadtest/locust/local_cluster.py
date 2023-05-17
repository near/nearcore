#!/usr/bin/env python3

import argparse
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[3] / 'lib'))

import cluster


def main():
    """
    Set up a local cluster to run locust against
    """
    parser = argparse.ArgumentParser(
        description='Run a local cluster for testing.')
    parser.add_argument('--shards', default=10, help='number of shards')
    args = parser.parse_args()

    config = cluster.load_config()
    nodes = cluster.start_cluster(
        2, 0, args.shards, config, [["epoch_length", 100]], {
            shard: {
                "tracked_shards": list(range(int(args.shards)))
            } for shard in range(int(args.shards) + 1)
        })

    for node in nodes:
        print(f"RPC node listening on port {node.rpc_port}")

    while True:
        continue


if __name__ == "__main__":
    main()
