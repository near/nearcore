"""
Protocol updates correct computation of inflation and total supply according to NEP.
Adding online fraction thresholds params and removing block/chunk kickout numbers.
No changes to state records.
"""
import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 11

config['protocol_version'] = 12
config['online_max_threshold'] = [99, 100]
config['online_min_threshold'] = [90, 100]
config['chunk_producer_kickout_threshold'] = 90

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
