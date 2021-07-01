#!/usr/bin/env python3

import argparse
import os

from nodelib import setup_and_run, initialize_keys, start_stakewars

if __name__ == "__main__":

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
        '--init',
        action='store_true',
        help=
        'If set, initialize the home dir by generating validator key and node key'
    )
    parser.add_argument(
        '--signer-keys',
        action='store_true',
        help='If set, generate signer keys for account specified')
    parser.add_argument(
        '--account-id',
        default='',
        help='If set, the account id will be used for running a validator')
    parser.add_argument(
        '--image',
        default='nearprotocol/nearcore:stakewars',
        help='Image to run in docker (default: nearprotocol/nearcore:stakewars)'
    )
    parser.add_argument('--tracked-shards',
                        default='',
                        help='The shards that this node wants to track')
    args = parser.parse_args()

    TELEMETRY_URL = 'https://explorer.tatooine.nearprotocol.com/api/nodes'

    if args.local:
        print("Flag --local deprecated, please use --nodocker")

    nodocker = args.nodocker or args.local
    if args.init:
        initialize_keys(args.home, not args.debug, nodocker, args.image,
                        args.account_id, args.signer_keys)
    else:
        print("****************************************************")
        print("* Running NEAR validator node for Stake Wars *")
        print("****************************************************")
        start_stakewars(args.home,
                        not args.debug,
                        nodocker,
                        args.image,
                        telemetry_url=TELEMETRY_URL,
                        verbose=args.verbose,
                        tracked_shards=args.tracked_shards)
