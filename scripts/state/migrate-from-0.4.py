#!/usr/bin/env python

# Migrate from 0.4 to 0.5 (VMLimitConfig).

import json
import re
import os
import base64

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())

q['protocol_version'] = 5
ext_costs = q['runtime_config']['wasm_config']['ext_costs']
wasm_config = {
    "ext_costs": ext_costs,
    "grow_mem_cost": 1,
    "regular_op_cost": 1,
    "limit_config": json.loads("""{"max_gas_burnt":200000000000000,"max_gas_burnt_view":200000000000000,"max_stack_height":16384,"initial_memory_pages":1024,"max_memory_pages":2048,"registers_memory_limit":1073741824,"max_register_size":104857600,"max_number_registers":100,"max_number_logs":100,"max_log_len":500,"max_total_prepaid_gas":10000000000000000,"max_actions_per_receipt":100,"max_number_bytes_method_names":2000,"max_length_method_name":256,"max_arguments_length":4194304,"max_length_returned_data":4194304,"max_contract_size":4194304,"max_length_storage_key":4194304,"max_length_storage_value":4194304,"max_promises_per_function_call_action":1024,"max_number_input_data_dependencies":128}"""),
}
q['runtime_config']['wasm_config'] = wasm_config

open(filename + '.v5', 'w').write(json.dumps(q, indent=2))
