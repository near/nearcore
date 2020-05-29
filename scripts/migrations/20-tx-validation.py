"""
Split TX validation into stateless and with a state. Allows to reject transactions early.
Protocol changes include:
* Add TX validation before TX forwarding

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

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
