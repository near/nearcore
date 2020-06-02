import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output_config.json')),
                   object_pairs_hook=OrderedDict)
records_fname = [
    filename for filename in os.listdir(home)
    if filename.startswith('output_records_')
]
assert len(records_fname) == 1, "Not found records file or found too many"

records = json.load(open(os.path.join(home, records_fname[0])))

assert config['protocol_version'] == 4

config['protocol_version'] = 5
config['genesis_height'] = 0
config['config_version'] = 1
config['num_block_producer_seats'] = 100
config['num_block_producer_seats_per_shard'] = [100]
config['avg_hidden_validator_seats_per_shard'] = [0]
config['epoch_length'] = 43200
config['transaction_validity_period'] = 86400
config['runtime_config']['transaction_costs']['storage_usage_config'] = {
    'num_bytes_account': 100,
    'num_extra_bytes_record': 40
}
config['runtime_config']['wasm_config'].pop('max_gas_burnt')
config['runtime_config']['wasm_config'].pop('max_stack_height')
config['runtime_config']['wasm_config'].pop('initial_memory_pages')
config['runtime_config']['wasm_config'].pop('max_memory_pages')
config['runtime_config']['wasm_config'].pop('registers_memory_limit')
config['runtime_config']['wasm_config'].pop('max_register_size')
config['runtime_config']['wasm_config'].pop('max_number_registers')
config['runtime_config']['wasm_config'].pop('max_number_logs')
config['runtime_config']['wasm_config'].pop('max_log_len')
config['runtime_config']['wasm_config']['limit_config'] = {
    "max_gas_burnt": 200000000000000,
    "max_gas_burnt_view": 200000000000000,
    "max_stack_height": 16384,
    "initial_memory_pages": 1024,
    "max_memory_pages": 2048,
    "registers_memory_limit": 1073741824,
    "max_register_size": 104857600,
    "max_number_registers": 100,
    "max_number_logs": 100,
    "max_total_log_length": 16384,
    "max_total_prepaid_gas": 10000000000000000,
    "max_actions_per_receipt": 100,
    "max_number_bytes_method_names": 2000,
    "max_length_method_name": 256,
    "max_arguments_length": 4194304,
    "max_length_returned_data": 4194304,
    "max_contract_size": 4194304,
    "max_length_storage_key": 4194304,
    "max_length_storage_value": 4194304,
    "max_promises_per_function_call_action": 1024,
    "max_number_input_data_dependencies": 128
}
config['records'] = records

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
