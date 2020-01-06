#!/usr/bin/env python

# Migrates from 0.4 to the latest version.
# When adding changes to the genesis config:
# - add a new version to the change log
# - implement the migration code using the following template:
# ```
# if config_version == 5:
#     # add migration code here
#     pass
#     # increment config version
#     config_version = 6
# ```
#
# Config version change log:
# - #1: Adds `limit_config` to `VMConfig` and introduces `config_version`.

import json
import os

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())

config_version = q.get('config_version', 0)

if config_version == 0:
    ext_costs = q['runtime_config']['wasm_config']['ext_costs']
    wasm_config = {
        "ext_costs": ext_costs,
        "grow_mem_cost": 1,
        "regular_op_cost": 1,
        "limit_config": json.loads("""{"max_gas_burnt":200000000000000,"max_gas_burnt_view":200000000000000,"max_stack_height":16384,"initial_memory_pages":1024,"max_memory_pages":2048,"registers_memory_limit":1073741824,"max_register_size":104857600,"max_number_registers":100,"max_number_logs":100,"max_log_len":500,"max_total_prepaid_gas":10000000000000000,"max_actions_per_receipt":100,"max_number_bytes_method_names":2000,"max_length_method_name":256,"max_arguments_length":4194304,"max_length_returned_data":4194304,"max_contract_size":4194304,"max_length_storage_key":4194304,"max_length_storage_value":4194304,"max_promises_per_function_call_action":1024,"max_number_input_data_dependencies":128}"""),
    }
    q['runtime_config']['wasm_config'] = wasm_config
    config_version = 1

# Add future migration code below, without removing the previous migration code.
# Use the following template:
#
# if config_version == 1:
#    ...
#    config_version = 2

# Update the config version in the testnet
q['config_version'] = config_version

# We overwrite the file instead of creating a new one.
# Too bad the file is too large so you can't see the difference in git.
open(filename, 'w').write(json.dumps(q, indent=2, sort_keys=True))
