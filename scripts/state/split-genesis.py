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
q = json.loads(open(filename).read(), object_pairs_hook=OrderedDict)

records = q['records']
q['records'] = []

open(os.path.join(os.path.dirname(filename), 'genesis_config.json'),
     'w').write(json.dumps(q, indent=2))
open(os.path.join(os.path.dirname(filename), '_genesis_records.json'),
     'w').write(json.dumps(records, indent=2))
