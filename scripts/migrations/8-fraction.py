"""
This migration implements https://github.com/nearprotocol/nearcore/issues/1747

Changes:
 - Change `protocol_reward_percentage`, `developer_reward_percentage`, and `max_inflation_rate` to fractions.
"""


import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')), object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 7

config['protocol_version'] = 8
config['gas_price_adjustment_rate'] = {
    'numerator': 1,
    'denominator': 100,
}
config['protocol_reward_percentage'] = {
    'numerator': 1,
    'denominator': 10,
}
config['developer_reward_percentage'] = {
    'numerator': 3,
    'denominator': 10,
}
config['max_inflation_rate'] = {
    'numerator': 5,
    'denominator': 100,
}


json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
