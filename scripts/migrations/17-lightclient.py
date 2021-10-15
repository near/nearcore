"""
Make the node adhere to the light client spec.
Protocol changes include:
* Change the way the `next_bp_hash` is computed
* Change the way the approvals are serialized for signatures

"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 16

config['protocol_version'] = 17

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
