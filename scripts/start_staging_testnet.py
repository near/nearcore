#!/usr/bin/env python3

import argparse
import os

from nodelib import setup_and_run

if __name__ == "__main__":
    print("****************************************************")
    print("* Running NEAR validator node for Staging TestNet *")
    print("****************************************************")

    DEFAULT_BOOT_NODE = ','.join([
        "HWVYveEYJThm7woXrbVctRjEb7QjpcWuQpM1JrMnFNcr@34.94.204.205:24567",
        "BndAG9whZjb99wYNChq2xaTMJHeCPMBiCjGNSTXWbRhf@35.236.26.48:24567",
        "Aksfwi3UXBAPjHhPQ9mCj387N6AcD5ixAi8bvVwvfv3G@104.198.217.61:24567",
    ])
    TELEMETRY_URL = 'https://explorer.staging.nearprotocol.com/api/nodes'

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
        default='nearprotocol/nearcore:staging',
        help='Image to run in docker (default: nearprotocol/nearcore:staging)')
    parser.add_argument('--boot-nodes',
                        default=DEFAULT_BOOT_NODE,
                        help='Specify boot nodes to load from (Default: %s)' %
                        DEFAULT_BOOT_NODE)
    args = parser.parse_args()

    if args.local:
        print("Flag --local deprecated, please use --nodocker")
    nodocker = args.nodocker or args.local
    setup_and_run(nodocker, not args.debug, args.image, args.home,
                  ['--chain-id=staging'], args.boot_nodes, TELEMETRY_URL,
                  args.verbose)
