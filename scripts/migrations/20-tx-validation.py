"""
Introduce minimum required attached gas
Protocol changes include:
* Introduce min_prepaid_gas

"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')), object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 19

config['protocol_version'] = 20
config['runtime_config']['wasm_config']['limit_config']['min_prepaid_gas'] = 1

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
