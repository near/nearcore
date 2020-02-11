#!/usr/bin/env python

# Migrate for eth bridge.

import json
import re
import os
import base64

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())

q['records'] = []
q['runtime_config']['wasm_config']['ext_costs']['keccak256_base'] = 710092630
q['runtime_config']['wasm_config']['ext_costs']['keccak256_byte'] = 5536829
q['runtime_config']['wasm_config']['ext_costs']['keccak512_base'] = 710092630 * 2
q['runtime_config']['wasm_config']['ext_costs']['keccak512_byte'] = 5536829 * 2

open(filename + '.new', 'w').write(json.dumps(q, indent=2))
