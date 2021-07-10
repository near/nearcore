#!/usr/bin/env python3

import argparse
import os

from nodelib import setup_and_run

if __name__ == "__main__":
    print("****************************************************")
    print("* Running NEAR validator node for Local TestNet    *")
    print("****************************************************")

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
    args = parser.parse_args()

    if args.local:
        print("Flag --local deprecated, please use --nodocker")
    nodocker = args.nodocker or args.local
    setup_and_run(nodocker,
                  not args.debug,
                  args.image,
                  args.home,
                  init_flags=[],
                  boot_nodes='',
                  telemetry_url='',
                  verbose=args.verbose)
