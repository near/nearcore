#!/usr/bin/env python

# Splits testnet.json into 2 files:
# - testnet_config.json - that contains all fields except for the records
# - testnet_records.json - that contains all records from the testnet.json
# It keeps key order in the genesis config file.

import json
import os
from collections import OrderedDict

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read(), object_pairs_hook=OrderedDict)

records = q['records']
q['records'] = []

open(filename.replace('.json', '_config.json'), 'w').write(json.dumps(q, indent=2))
open(filename.replace('.json', '_records.json'), 'w').write(json.dumps(records, indent=2))
