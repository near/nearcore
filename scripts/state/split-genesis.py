#!/usr/bin/env python

# Splits testnet.json into 2 files:
# - testnet.json.config - that contains all fields except for the records
# - testnet.json.records - that contains all records from the testnet.json
# It also sorts keys in the genesis config file.
# NOTE: Python 3.7 required for sort_keys

import json
import os

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())

records = q['records']
q['records'] = []

open(filename + '.config', 'w').write(json.dumps(q, indent=2, sort_keys=True))
open(filename + '.records', 'w').write(json.dumps(records, indent=2))
