"""
Switching Receipt `gas_price` to be a pessimistic gas price.
It's computed using the total prepaid gas and the inflation using `pessimistic_gas_price_inflation_ratio`.
- add `pessimistic_gas_price_inflation_ratio`
- change `max_total_prepaid_gas` to `3 * 10**14` from `10**16`, so the maximum depth is 64.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 25

config['protocol_version'] = 26

config['runtime_config']['transaction_costs'][
    'pessimistic_gas_price_inflation_ratio'] = [103, 100]
config['runtime_config']['wasm_config']['limit_config'][
    'max_total_prepaid_gas'] = 300000000000000

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
