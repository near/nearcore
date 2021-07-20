#!/usr/bin/env python3

# Migrates from 0.4 to the latest version.
# When adding changes to the genesis config:
# - add a new version to the change log
# - implement the migration code using the following template:
# ```
# if config_version == 5:
#     # add migration code here
#     pass
#     # increment config version
#     config_version = 6
# ```
#
# Config version change log:
# - #1: Replaces `runtime_config` to use defaults and introduces `config_version`.

import json
import os

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        '../../near/res/testnet.json')
q = json.loads(open(filename).read())

config_version = q.get('config_version', 0)

if config_version == 0:
    num_sec_per_year = 31556952
    # The rest of `runtime_config` fields are default
    q['runtime_config'] = {
        'poke_threshold':
            24 * 3600,
        'storage_cost_byte_per_block':
            str(5 * 10**6),
        'account_length_baseline_cost_per_block':
            str(10**24 * 3**8 // num_sec_per_year),
    }
    config_version = 1

# Add future migration code below, without removing the previous migration code.
# Use the following template:
#
# if config_version == 1:
#    ...
#    config_version = 2

# Update the config version in the testnet
q['config_version'] = config_version

# We overwrite the file instead of creating a new one.
open(filename, 'w').write(json.dumps(q, indent=2, sort_keys=True))

# Dump the config into a separate file for easier reviewing in the git.
# It's not used for the reading genesis.
del q['records']
open(filename + '.config', 'w').write(json.dumps(q, indent=2, sort_keys=True))
