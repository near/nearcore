#!/usr/bin/env python3

import argparse
import os
import subprocess

from nodelib import setup_and_run

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--local',
        action='store_true',
        help=
        'If set, runs in the local version instead of auto-updatable docker. Otherwise runs locally'
    )
    parser.add_argument('--release',
                        action='store_true',
                        help='If set, compiles nearcore in release mode')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='If set, prints verbose logs')
    parser.add_argument(
        '--image',
        default='nearprotocol/nearcore',
        help='Image to run in docker (default: nearprotocol/nearcore)')
    args = parser.parse_args()

    print(
        "Starting unittest nodes with test.near account and seed key of alice.near"
    )
    home_dir = os.path.join(os.getcwd(), 'testdir', '.near')
    subprocess.call(['rm', '-rf', home_dir])
    subprocess.call(['mkdir', '-p', home_dir])
    setup_and_run(args.local,
                  args.release,
                  args.image,
                  home_dir,
                  init_flags=[
                      '--test-seed=alice.near', '--account-id=test.near',
                      '--fast'
                  ],
                  boot_nodes='',
                  telemetry_url='',
                  verbose=args.verbose)
