#!/usr/bin/python

import argparse
import subprocess

from nodelib import setup_and_run


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true', help='If set, runs in the local version instead of auto-updatable docker. Otherwise runs locally')
    parser.add_argument(
        '--image', default='nearprotocol/nearcore',
        help='Image to run in docker (default: nearprotocol/nearcore)')
    args = parser.parse_args()

    print("Starting unittest nodes with test.near account and seed key of alice.near")
    subprocess.call(['rm', '-rf', 'testdir'])
    setup_and_run(args.local, args.image, 'testdir', ['--chain-id=', '--test-seed=alice.near', '--account-id=test.near', '--fast'], '')
