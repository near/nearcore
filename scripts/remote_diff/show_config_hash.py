#!/usr/bin/env python3
"""
Shows md5sum of /home/ubuntu/.near/config.json on provided google cloud machines.

Usage: ./show_config_hash.py project host1 host2 host3 ...
Example for testnet canaries:
    ./show_config_hash.py near-core testnet-canary-rpc-01-europe-north1-a-1f3e1e97 \
    testnet-canary-rpc-02-europe-west2-a-031e15e8 testnet-canary-rpc-archive-01-asia-east2-a-b25465d1 \
    testnet-canary-validator-01-us-west1-a-f160e149
"""
import sys
from utils import display_table, run_on_machine


def install_jq(project, host, user='ubuntu'):
    run_on_machine("sudo apt-get install jq -y", user, host, project)


def get_canonical_md5sum(project, host, user='ubuntu'):
    install_jq(project, host, user)
    return run_on_machine("jq --sort-keys . ~/.near/config.json | md5sum", user,
                          host, project)


def display_hashes(names, hashes):
    rows = sorted(zip(names, hashes), key=lambda x: x[1])
    display_table([("name", "hash")] + rows)


if __name__ == '__main__':
    project = sys.argv[1]
    hosts = sys.argv[2:]
    md5sums = [get_canonical_md5sum(project, host) for host in hosts]
    display_hashes(hosts, md5sums)
