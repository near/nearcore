#!/usr/bin/env python3
"""
Shows results of '/home/ubuntu/neard -V' on provided google cloud machines.
Usage: ./show_neard_version.py project host1 host2 host3 ...
Example for testnet canaries:
    ./show_neard_version.py near-core testnet-canary-rpc-01-europe-north1-a-1f3e1e97 \
    testnet-canary-rpc-02-europe-west2-a-031e15e8 testnet-canary-rpc-archive-01-asia-east2-a-b25465d1 \
    testnet-canary-validator-01-us-west1-a-f160e149
"""
import sys
from utils import display_table, run_on_machine


def get_neard_info(project, host, user='ubuntu'):
    return run_on_machine("./neard -V", user, host, project)


def display_neard_info(hosts, neard_info, user='ubuntu'):
    display_table([[host] + neard_info.split(' ')[1:]
                   for (host, neard_info) in zip(hosts, neard_infos)])


if __name__ == '__main__':
    project = sys.argv[1]
    hosts = sys.argv[2:]
    neard_infos = [get_neard_info(project, host) for host in hosts]
    display_neard_info(hosts, neard_infos)
