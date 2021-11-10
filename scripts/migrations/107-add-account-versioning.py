"""
Migrating to versioned Account
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] < 107

config['protocol_version'] = 107

for record in config['records']:
    if record.get('Account'):
        record['Account']['account'] = {
            'AccountV1': record['Account']['account']
        }

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
