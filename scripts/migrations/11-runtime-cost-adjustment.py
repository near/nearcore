"""
Changes runtime costs based on results from the new runtime parameter estimator.

"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 10

config['protocol_version'] = 11
config['runtime_config']['transaction_costs'] = {
    "action_receipt_creation_config": {
        "send_sir": 108059500000,
        "send_not_sir": 108059500000,
        "execution": 108059500000
    },
    "data_receipt_creation_config": {
        "base_cost": {
            "send_sir": 4697339419375,
            "send_not_sir": 4697339419375,
            "execution": 4697339419375
        },
        "cost_per_byte": {
            "send_sir": 59357464,
            "send_not_sir": 59357464,
            "execution": 59357464
        }
    },
    "action_creation_config": {
        "create_account_cost": {
            "send_sir": 99607375000,
            "send_not_sir": 99607375000,
            "execution": 99607375000
        },
        "deploy_contract_cost": {
            "send_sir": 184765750000,
            "send_not_sir": 184765750000,
            "execution": 184765750000
        },
        "deploy_contract_cost_per_byte": {
            "send_sir": 6812999,
            "send_not_sir": 6812999,
            "execution": 6812999
        },
        "function_call_cost": {
            "send_sir": 2319861500000,
            "send_not_sir": 2319861500000,
            "execution": 2319861500000
        },
        "function_call_cost_per_byte": {
            "send_sir": 2235934,
            "send_not_sir": 2235934,
            "execution": 2235934
        },
        "transfer_cost": {
            "send_sir": 115123062500,
            "send_not_sir": 115123062500,
            "execution": 115123062500
        },
        "stake_cost": {
            "send_sir": 141715687500,
            "send_not_sir": 141715687500,
            "execution": 102217625000
        },
        "add_key_cost": {
            "full_access_cost": {
                "send_sir": 101765125000,
                "send_not_sir": 101765125000,
                "execution": 101765125000
            },
            "function_call_cost": {
                "send_sir": 102217625000,
                "send_not_sir": 102217625000,
                "execution": 102217625000
            },
            "function_call_cost_per_byte": {
                "send_sir": 1925331,
                "send_not_sir": 1925331,
                "execution": 1925331
            }
        },
        "delete_key_cost": {
            "send_sir": 94946625000,
            "send_not_sir": 94946625000,
            "execution": 94946625000
        },
        "delete_account_cost": {
            "send_sir": 147489000000,
            "send_not_sir": 147489000000,
            "execution": 147489000000
        }
    },
    "storage_usage_config": {
        "num_bytes_account": 100,
        "num_extra_bytes_record": 40
    },
    "burnt_gas_reward": [3, 10]
}
config['runtime_config']['wasm_config']['ext_costs'] = {
    "base": 265261758,
    "read_memory_base": 2584050225,
    "read_memory_byte": 3801396,
    "write_memory_base": 2780731725,
    "write_memory_byte": 2723859,
    "read_register_base": 2493624561,
    "read_register_byte": 98622,
    "write_register_base": 2840975211,
    "write_register_byte": 3801645,
    "utf8_decoding_base": 3110963061,
    "utf8_decoding_byte": 289342653,
    "utf16_decoding_base": 3593689800,
    "utf16_decoding_byte": 167519322,
    "sha256_base": 4530046500,
    "sha256_byte": 24116301,
    "keccak256_base": 5867223186,
    "keccak256_byte": 21469644,
    "keccak512_base": 5798128650,
    "keccak512_byte": 36651981,
    "log_base": 2408221236,
    "log_byte": 15863835,
    "storage_write_base": 45187219125,
    "storage_write_key_byte": 66445653,
    "storage_write_value_byte": 29682120,
    "storage_write_evicted_byte": 28939782,
    "storage_read_base": 32029296375,
    "storage_read_key_byte": 28463997,
    "storage_read_value_byte": 3289884,
    "storage_remove_base": 35876668875,
    "storage_remove_key_byte": 35342424,
    "storage_remove_ret_value_byte": 7303842,
    "storage_has_key_base": 31315025250,
    "storage_has_key_byte": 28376217,
    "storage_iter_create_prefix_base": 0,
    "storage_iter_create_prefix_byte": 0,
    "storage_iter_create_range_base": 0,
    "storage_iter_create_from_byte": 0,
    "storage_iter_create_to_byte": 0,
    "storage_iter_next_base": 0,
    "storage_iter_next_key_byte": 0,
    "storage_iter_next_value_byte": 0,
    "touching_trie_node": 5764118571,
    "promise_and_base": 1473816795,
    "promise_and_per_promise": 5613432,
    "promise_return": 558292404
}

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
