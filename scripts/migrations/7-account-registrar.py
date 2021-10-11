"""
This migration implements state staking spec change:
https://github.com/nearprotocol/NEPs/pull/45

Changes:
 - Introduce `account_creation_config` in `RuntimeConfig`.
 - Set `min_allowed_top_level_account_length` to 0. Means any top-level account ID can still be created by anyone.
 - Set `registrar_account_id` to `registrar`.
 - Creates a new `registrar` account with `near` access keys and 1M $N.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 6

config['protocol_version'] = 7
config['runtime_config']['account_creation_config'] = {
    'min_allowed_top_level_account_length': 0,
    'registrar_account_id': 'registrar',
}

records = []

near_access_key_records = []

# Removing existing `registrar` account.
for record in config['records']:
    if ('Account' in record) and (record['Account']['account_id']
                                  == 'registrar'):
        continue
    if ('AccessKey' in record) and (record['AccessKey']['account_id']
                                    == 'registrar'):
        continue
    if ('AccessKey' in record) and (record['AccessKey']['account_id']
                                    == 'near'):
        near_access_key_records.append(record['AccessKey'])
    records.append(record)

assert (len(near_access_key_records) > 0)

records.append({
    'Account': {
        'account': {
            # 1_000_000 N or 1e30
            'amount': str(10**30),
            'locked': '0',
            'code_hash': '11111111111111111111111111111111',
            'storage_usage': 100 + len(near_access_key_records) * 82,
        },
        'account_id': 'registrar',
    }
})

for access_key_record in near_access_key_records:
    records.append({
        'AccessKey': {
            'access_key': access_key_record['access_key'],
            'public_key': access_key_record['public_key'],
            'account_id': 'registrar',
        }
    })

config['records'] = records

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
