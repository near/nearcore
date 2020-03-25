import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output_config.json')), object_pairs_hook=OrderedDict)
records_fname = [filename for filename in os.listdir(home) if filename.startswith('output_records_')]
assert len(records_fname) == 1, "Not found records file or found too many"

records = json.load(open(os.path.join(home, records_fname[0])))

assert config['protocol_version'] == 4

config['genesis_height'] = 0
config['records'] = records
config['protocol_version'] = 5

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)

