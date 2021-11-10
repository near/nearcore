"""
Expose host functions for retrieving validator stake.

No state migration needed for this change

"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 15

config['protocol_version'] = 16

config['runtime_config']['wasm_config']['ext_costs'][
    'validator_stake_base'] = 303944908800
config['runtime_config']['wasm_config']['ext_costs'][
    'validator_total_stake_base'] = 303944908800

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
