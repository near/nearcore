"""
Changes gas price computation costs. No state migration needed for this change

"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')), object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 10

config['protocol_version'] = 11

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
