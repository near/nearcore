#!/usr/bin/env python3

# Splits given genesis.json into 2 files:
# - genesis_config.json - that contains all fields except for the records
# - genesis_records.json - that contains all records from the genesis.json
# It keeps key order in the genesis config file.

import json
import os
import sys
from collections import OrderedDict

filename = sys.argv[1]
with open(filename) as fd:
    q = json.load(fd, object_pairs_hook=OrderedDict)

records = q['records']
q['records'] = []

dirname = os.path.dirname(filename)
with open(os.path.join(dirname, 'genesis_config.json'), 'w') as fd:
    json.dump(q, fd, indent=2)
with open(os.path.join(dirname, '_genesis_records.json'), 'w') as fd:
    json.dump(records, fd, indent=2)
