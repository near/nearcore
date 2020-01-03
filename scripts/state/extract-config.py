#!/usr/bin/env python

# Python 3.7 required for sort_keys
# Extract all fields except for the records.

import json
import os

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())

records = q['records']
q['records'] = []


open(filename + '.config', 'w').write(json.dumps(q, indent=2, sort_keys=True))
open(filename + '.data', 'w').write(json.dumps(records, indent=2))
