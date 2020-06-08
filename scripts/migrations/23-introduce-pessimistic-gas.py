"""
Switching Receipt `gas_price` to be a pessimistic gas price.
It's computed using the total prepaid gas and the inflation using `pessimistic_gas_price_inflation_ratio`.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')), object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 22

config['protocol_version'] = 23

config.pop('config_version')
config['pessimistic_gas_price_inflation_ratio'] = [103, 100]

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
