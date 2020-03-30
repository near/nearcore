"""
This migration implements state staking spec change:
https://github.com/nearprotocol/NEPs/pull/41

Full discussion can be found:
https://github.com/nearprotocol/NEPs/issues/40

Changes:
 - Replace `storage_cost_byte_per_block` with `storage_amount_per_byte` for state staking.
 - Remove `account_length_baseline_cost_per_block` as we don't charge for account names anymore.
 - Remove `storage_paid_at` in Account as we now don't need to maintain virtual balances.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')), object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 5

config['protocol_version'] = 6
config['runtime_config'].pop('storage_cost_byte_per_block')
config['runtime_config'].pop('account_length_baseline_cost_per_block')
config['runtime_config']['storage_amount_per_byte'] = "90949470177292823791"

for record in config['records']:
    if "Account" in record:
        record["Account"]["account"].pop("storage_paid_at")

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
