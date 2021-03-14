#!/usr/bin/env python3
## Take runtime_config.json output from runtime-param-estimator, and apply
## safety mutiplier to some costs, output is used as genesis.runtime_config

import json
import sys
from collections import OrderedDict

if len(sys.argv) >= 2:
    input_runtime_config = sys.argv[1]
else:
    input_runtime_config = '/tmp/data/runtime_config.json'

with open(input_runtime_config) as f:
    runtime_config = json.load(f, object_pairs_hook=OrderedDict)

for k, v in runtime_config['wasm_config']['ext_costs'].items():
    runtime_config['wasm_config']['ext_costs'][k] = int(v) * 3
runtime_config['wasm_config']['regular_op_cost'] = int(
    runtime_config['wasm_config']['regular_op_cost']) * 3

json.dump(runtime_config,
          open(input_runtime_config.replace('.json', '_safety_mutiplied.json'),
               'w'),
          indent=2)
