"""
Add minimum stake divisor so that if the stake of a staking transaction is too small,
it will be rejected.

"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 24

config['protocol_version'] = 25
config['minimum_stake_divisor'] = 10

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
