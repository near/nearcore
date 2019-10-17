#!/usr/bin/env python

import argparse
import os

from nodelib import setup_and_run


if __name__ == "__main__":
    print("****************************************************")
    print("* Running NEAR validator node for Official TestNet *")
    print("****************************************************")

    DEFAULT_BOOT_NODE = "49ppQ9vkLYvWajZ1KRMdism4AQswFT4yD2e9kRt7B4rC@34.94.33.164:24567"

    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true', help='deprecated: use --nodocker')
    parser.add_argument('--nodocker', action='store_true', help='If set, compiles and runs the node on the machine directly (not inside the docker).')
    parser.add_argument('--debug', action='store_true', help='If set, compiles local nearcore in debug mode')
    parser.add_argument('--verbose', action='store_true', help='If set, prints verbose logs')
    parser.add_argument('--home', default=os.path.expanduser('~/.near/'), help='Home path for storing configs, keys and chain data (Default: ~/.near)')
    parser.add_argument(
        '--image', default='nearprotocol/nearcore',
        help='Image to run in docker (default: nearprotocol/nearcore)')
    parser.add_argument(
        '--boot-nodes', default=DEFAULT_BOOT_NODE,
        help='Specify boot nodes to load from (Default: %s)' % DEFAULT_BOOT_NODE)
    args = parser.parse_args()

    if args.local:
        print("Flag --local deprecated, please use --nodocker")
    nodocker = args.nodocker or args.local
    setup_and_run(nodocker, not args.debug, args.image, args.home, ['--chain-id=testnet'],
                  args.boot_nodes, args.verbose)
