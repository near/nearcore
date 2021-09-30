#!/usr/bin/env python3

# update_res.py: update nearcore/res/genesis_config.json to be current `near init` without records
# update_res.py check: check nearcore/res/genesis_config.json matches current `near init`

from collections import OrderedDict
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile


def main():
    if len(sys.argv) == 1:
        update_res()
    elif len(sys.argv) == 2 and sys.argv[1] == 'check':
        check_res()
    else:
        print('Usage: update-res.py | update-res.py check')
        exit(2)


genesis_config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   '../../nearcore/res/genesis_config.json')


def near_init_genesis():
    tempdir = pathlib.Path(tempfile.gettempdir()) / 'update_res'
    if tempdir.exists():
        shutil.rmtree(tempdir)
    tempdir.mkdir(parents=True, exist_ok=True)

    subprocess.check_output(('cargo', 'run', '-p', 'neard', '--bin', 'neard',
                             '--' ,'--home', tempdir, 'init' ,'--chain-id',
                             'sample'))
    genesis = json.load(open(tempdir / 'genesis.json'),
                        object_pairs_hook=OrderedDict)
    genesis['records'] = []
    # To avoid nearcore/res/genesis_config.json doesn't change everytime
    genesis['genesis_time'] = '1970-01-01T00:00:00.000000000Z'
    # secret key is seed from test.near
    genesis['validators'][0][
        'public_key'] = 'ed25519:9BmAFNRTa5mRRXpSAm6MxSEeqRASDGNh2FuuwZ4gyxTw'
    return genesis


def update_res():
    genesis = near_init_genesis()
    json.dump(genesis, open(genesis_config_path, 'w'), indent=2)
    print('nearcore/res/genesis_config.json updated')


def check_res():
    genesis = near_init_genesis()
    res_genesis_config = json.load(open(genesis_config_path),
                                   object_pairs_hook=OrderedDict)
    if genesis != res_genesis_config:
        print(
            'nearcore/res/genesis_config.json does not match `near init` generated'
        )
        print('Please update by run scripts/state/update_res.py')
        exit(1)


if __name__ == '__main__':
    main()
