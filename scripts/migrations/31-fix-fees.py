"""
Adding `contract_compile_base` and `contract_compile_byte` fees.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 29 or config['protocol_version'] == 30

config['protocol_version'] = 31

config['runtime_config'] = {
    "storage_amount_per_byte": "90900000000000000000",
    "transaction_costs": {
        "action_receipt_creation_config": {
            "send_sir": 151021812500,
            "send_not_sir": 151021812500,
            "execution": 151021812500
        },
        "data_receipt_creation_config": {
            "base_cost": {
                "send_sir": 1068762938750,
                "send_not_sir": 1068762938750,
                "execution": 1068762938750
            },
            "cost_per_byte": {
                "send_sir": 23982274,
                "send_not_sir": 23982274,
                "execution": 23982274
            }
        },
        "action_creation_config": {
            "create_account_cost": {
                "send_sir": 130476125000,
                "send_not_sir": 130476125000,
                "execution": 130476125000
            },
            "deploy_contract_cost": {
                "send_sir": 222739562500,
                "send_not_sir": 222739562500,
                "execution": 222739562500
            },
            "deploy_contract_cost_per_byte": {
                "send_sir": 6846508,
                "send_not_sir": 6846508,
                "execution": 6846508
            },
            "function_call_cost": {
                "send_sir": 2614094875000,
                "send_not_sir": 2614094875000,
                "execution": 2614094875000
            },
            "function_call_cost_per_byte": {
                "send_sir": 2521519,
                "send_not_sir": 2521519,
                "execution": 2521519
            },
            "transfer_cost": {
                "send_sir": 159813250000,
                "send_not_sir": 159813250000,
                "execution": 159813250000
            },
            "stake_cost": {
                "send_sir": 167840812500,
                "send_not_sir": 167840812500,
                "execution": 167840812500
            },
            "add_key_cost": {
                "full_access_cost": {
                    "send_sir": 137640187500,
                    "send_not_sir": 137640187500,
                    "execution": 137640187500
                },
                "function_call_cost": {
                    "send_sir": 135268437500,
                    "send_not_sir": 135268437500,
                    "execution": 135268437500
                },
                "function_call_cost_per_byte": {
                    "send_sir": 22361438,
                    "send_not_sir": 22361438,
                    "execution": 22361438
                }
            },
            "delete_key_cost": {
                "send_sir": 122405750000,
                "send_not_sir": 122405750000,
                "execution": 122405750000
            },
            "delete_account_cost": {
                "send_sir": 205135750000,
                "send_not_sir": 205135750000,
                "execution": 205135750000
            }
        },
        "storage_usage_config": {
            "num_bytes_account": 100,
            "num_extra_bytes_record": 40
        },
        "burnt_gas_reward": [3, 10],
        "pessimistic_gas_price_inflation_ratio": [103, 100]
    },
    "wasm_config": {
        "ext_costs": {
            "base": 253725981,
            "contract_compile_base": 35501184,
            "contract_compile_bytes": 220125,
            "read_memory_base": 1505716125,
            "read_memory_byte": 3801027,
            "write_memory_base": 1700382561,
            "write_memory_byte": 2723103,
            "read_register_base": 1414060275,
            "read_register_byte": 97521,
            "write_register_base": 1763102361,
            "write_register_byte": 3801273,
            "utf8_decoding_base": 2010751611,
            "utf8_decoding_byte": 291541908,
            "utf16_decoding_base": 2440652175,
            "utf16_decoding_byte": 163512405,
            "sha256_base": 3438584625,
            "sha256_byte": 24047883,
            "keccak256_base": 4775015361,
            "keccak256_byte": 21363255,
            "keccak512_base": 4707189111,
            "keccak512_byte": 36541851,
            "log_base": 2440652175,
            "log_byte": 13127946,
            "storage_write_base": 52847227500,
            "storage_write_key_byte": 69602088,
            "storage_write_value_byte": 52939467,
            "storage_write_evicted_byte": 31047948,
            "storage_read_base": 44373578250,
            "storage_read_key_byte": 30115065,
            "storage_read_value_byte": 4565754,
            "storage_remove_base": 42990237750,
            "storage_remove_key_byte": 37322058,
            "storage_remove_ret_value_byte": 9205245,
            "storage_has_key_base": 42497864625,
            "storage_has_key_byte": 29839980,
            "storage_iter_create_prefix_base": 0,
            "storage_iter_create_prefix_byte": 0,
            "storage_iter_create_range_base": 0,
            "storage_iter_create_from_byte": 0,
            "storage_iter_create_to_byte": 0,
            "storage_iter_next_base": 0,
            "storage_iter_next_key_byte": 0,
            "storage_iter_next_value_byte": 0,
            "touching_trie_node": 12678165213,
            "promise_and_base": 1355625270,
            "promise_and_per_promise": 5474205,
            "promise_return": 450309708,
            "validator_stake_base": 911834726400,
            "validator_total_stake_base": 911834726400
        },
        "grow_mem_cost": 1,
        "regular_op_cost": 3443562,
        "limit_config": {
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
            "max_total_prepaid_gas": 300000000000000,
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
    },
    "account_creation_config": {
        "min_allowed_top_level_account_length": 0,
        "registrar_account_id": "registrar"
    }
}

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
