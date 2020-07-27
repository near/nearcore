"""
This migration implements spec change https://github.com/nearprotocol/NEPs/pull/58.

Changes:
 - Change `gas_price_adjustment_rate`, `protocol_reward_percentage`, `developer_reward_percentage`,
 and `max_inflation_rate` to fractions.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 7

config['protocol_version'] = 8
config['gas_price_adjustment_rate'] = [1, 100]
config.pop('protocol_reward_percentage')
config.pop('developer_reward_percentage')
config['protocol_reward_rate'] = [1, 10]
config['max_inflation_rate'] = [1, 20]
config['runtime_config']['transaction_costs']['burnt_gas_reward'] = [3, 10]

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
