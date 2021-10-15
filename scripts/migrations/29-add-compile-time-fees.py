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

assert config['protocol_version'] == 28

config['protocol_version'] = 29

config['runtime_config']['wasm_config']['ext_costs'] = {
    "base": 264768111,
    "contract_compile_base": 35445963,
    "contract_compile_bytes": 216750,
    "read_memory_base": 2609863200,
    "read_memory_byte": 3801333,
    "write_memory_base": 2803794861,
    "write_memory_byte": 2723772,
    "read_register_base": 2517165186,
    "read_register_byte": 98562,
    "write_register_base": 2865522486,
    "write_register_byte": 3801564,
    "utf8_decoding_base": 3111779061,
    "utf8_decoding_byte": 291580479,
    "utf16_decoding_base": 3543313050,
    "utf16_decoding_byte": 163577493,
    "sha256_base": 4540970250,
    "sha256_byte": 24117351,
    "keccak256_base": 5879491275,
    "keccak256_byte": 21471105,
    "keccak512_base": 5811388236,
    "keccak512_byte": 36649701,
    "log_base": 3543313050,
    "log_byte": 13198791,
    "storage_write_base": 64196736000,
    "storage_write_key_byte": 70482867,
    "storage_write_value_byte": 31018539,
    "storage_write_evicted_byte": 32117307,
    "storage_read_base": 56356845750,
    "storage_read_key_byte": 30952533,
    "storage_read_value_byte": 5611005,
    "storage_remove_base": 53473030500,
    "storage_remove_key_byte": 38220384,
    "storage_remove_ret_value_byte": 11531556,
    "storage_has_key_base": 54039896625,
    "storage_has_key_byte": 30790845,
    "storage_iter_create_prefix_base": 0,
    "storage_iter_create_prefix_byte": 0,
    "storage_iter_create_range_base": 0,
    "storage_iter_create_from_byte": 0,
    "storage_iter_create_to_byte": 0,
    "storage_iter_next_base": 0,
    "storage_iter_next_key_byte": 0,
    "storage_iter_next_value_byte": 0,
    "touching_trie_node": 16101955926,
    "promise_and_base": 1465013400,
    "promise_and_per_promise": 5452176,
    "promise_return": 560152386,
    "validator_stake_base": 911834726400,
    "validator_total_stake_base": 911834726400
}

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
