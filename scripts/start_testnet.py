#!/usr/bin/env python3

import argparse
import os

from nodelib import setup_and_run

if __name__ == "__main__":
    print("****************************************************")
    print("* Running NEAR validator node for Official TestNet *")
    print("****************************************************")

    DEFAULT_BOOT_NODE = ','.join([
        "AJLCcX4Uymeq5ssavjUCyEA8SV6Y365Mh5h4shqMSTDA@34.94.190.204:24567",
        "EY9mX5FYyR1sqrGwkqCbUrmjgAtXs4DeNaf1sjG9MrkY@35.226.146.230:24567",
        "8K7NG5v2yvSq4A1wQuqSNvyY334BVq3ohvdu9wgpgjLG@104.154.188.160:24567",
        "FNCMYTt9Gexq6Nq3Z67gRX7eeZAh27swd1nrwN3smT9Q@35.246.133.183:24567",
    ])
    TELEMETRY_URL = 'https://explorer.nearprotocol.com/api/nodes'

    parser = argparse.ArgumentParser()
    parser.add_argument('--local',
                        action='store_true',
                        help='deprecated: use --nodocker')
    parser.add_argument(
        '--nodocker',
        action='store_true',
        help=
        'If set, compiles and runs the node on the machine directly (not inside the docker).'
    )
    parser.add_argument('--debug',
                        action='store_true',
                        help='If set, compiles local nearcore in debug mode')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='If set, prints verbose logs')
    parser.add_argument(
        '--home',
        default=os.path.expanduser('~/.near/'),
        help=
        'Home path for storing configs, keys and chain data (Default: ~/.near)')
    parser.add_argument(
        '--image',
        default='nearprotocol/nearcore',
        help='Image to run in docker (default: nearprotocol/nearcore)')
    parser.add_argument('--boot-nodes',
                        default=DEFAULT_BOOT_NODE,
                        help='Specify boot nodes to load from (Default: %s)' %
                        DEFAULT_BOOT_NODE)
    args = parser.parse_args()

    if args.local:
        print("Flag --local deprecated, please use --nodocker")
    nodocker = args.nodocker or args.local
    setup_and_run(nodocker,
                  not args.debug,
                  args.image,
                  args.home,
                  init_flags=['--chain-id=testnet'],
                  boot_nodes=args.boot_nodes,
                  telemetry_url=TELEMETRY_URL,
                  verbose=args.verbose)
