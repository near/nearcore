#!/usr/bin/python

import argparse
import os

from nodelib import setup_and_run


if __name__ == "__main__":
    print("****************************************************")
    print("* Running NEAR validator node for Local TestNet    *")
    print("****************************************************")

    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true', help='If set, runs in the local version instead of auto-updatable docker. Otherwise runs locally')
    parser.add_argument('--home', default=os.path.expanduser('~/.near/'), help='Home path for storing configs, keys and chain data (Default: ~/.near)')
    parser.add_argument(
        '--image', default='nearprotocol/nearcore',
        help='Image to run in docker (default: nearprotocol/nearcore)')
    args = parser.parse_args()

    setup_and_run(args.local, args.image, args.home, ['--chain-id='], '')
