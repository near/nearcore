#!/usr/bin/env python3
"""Checks or updates contents of nearcore/res/genesis_config.json file.

* `update_res.py` updates nearcore/res/genesis_config.json to current
  `near init` version without any records.

* `update_res.py check` checks whether nearcore/res/genesis_config.json
  file matches what `near init` generates.
"""

import collections
import json
import os
import pathlib
import subprocess
import sys
import tempfile


def main():
    if len(sys.argv) == 1:
        update_res()
    elif len(sys.argv) == 2 and sys.argv[1] == 'check':
        check_res()
    else:
        sys.exit(__doc__.rstrip())


GENESIS_REPO_PATH = 'chain/jsonrpc/jsonrpc-tests/res/genesis_config.json'
REPO_FULL_PATH = pathlib.Path(__file__).resolve().parent.parent.parent
GENESIS_FULL_PATH = REPO_FULL_PATH / GENESIS_REPO_PATH
SCRIPT_REPO_PATH = pathlib.Path(__file__).resolve().relative_to(REPO_FULL_PATH)


def near_init_genesis():
    with tempfile.TemporaryDirectory() as tempdir:
        subprocess.check_call(
            ('cargo', 'run', '-p', 'neard', '--bin', 'neard', '--', '--home',
             tempdir, 'init', '--chain-id', 'sample'))
        with open(os.path.join(tempdir, 'genesis.json')) as rd:
            genesis = json.load(rd, object_pairs_hook=collections.OrderedDict)
    genesis['records'] = []
    # To avoid the genesis config changing each time
    genesis['genesis_time'] = '1970-01-01T00:00:00.000000000Z'
    # Secret key is seeded from test.near
    genesis['validators'][0][
        'public_key'] = 'ed25519:9BmAFNRTa5mRRXpSAm6MxSEeqRASDGNh2FuuwZ4gyxTw'
    return genesis


def update_res():
    genesis = near_init_genesis()
    with open(GENESIS_FULL_PATH, 'w') as wr:
        json.dump(genesis, wr, indent=2)
    print(f'{GENESIS_REPO_PATH} updated')


def check_res():
    want_genesis = near_init_genesis()
    with open(GENESIS_FULL_PATH) as rd:
        got_genesis = json.load(rd)
    if want_genesis != got_genesis:
        sys.exit(
            f'`{GENESIS_REPO_PATH}` does not match `near init` generated one\n'
            f'Please update by running `{SCRIPT_REPO_PATH}` script')


if __name__ == '__main__':
    main()
