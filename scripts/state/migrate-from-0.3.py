#!/usr/bin/env python

# Migrate from 0.3.x state to 0.4 (sharded).

import json
import re
import os
import base64

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())
new_validators = [json.loads('''{
      "public_key": "ed25519:7rNEmDbkn8grQREdTt3PWhR1phNtsqJdgfV26XdR35QL",
      "amount": "20000000000000000000",
      "account_id": "far"
    }''')]
new_records = []

q['validators'] = new_validators
q['records'] = q['records'][0]

# The new rule for accounts IDs is excluding `@`
is_valid_account = re.compile(r"^(([a-z\d]+[\-_])*[a-z\d]+\.)*([a-z\d]+[\-_])*[a-z\d]+$")
assert(is_valid_account.match("near"))
far_account = json.loads('''{
                         "Account": {
    "account": {
        "locked": "0",
        "storage_usage": 182,
        "code_hash": "11111111111111111111111111111111",
        "amount": "10000000000000000000000000000000",
        "storage_paid_at": 0
    },
    "account_id": "far"
}
}''')
far_access_key = json.loads('''{
      "AccessKey": {
        "access_key": {
          "nonce": 0,
          "permission": "FullAccess"
        },
        "public_key": "ed25519:32zVgoqtuyRuDvSMZjWQ774kK36UTwuGRZMmPsS6xpMy",
        "account_id": "far"
      }
    }''')
new_records.append(far_account)
new_records.append(far_access_key)

for value in q['records']:
    if 'Account' in value:
        account_id = value['Account']['account_id']
        if not is_valid_account.match(account_id):
            print("Nuked account for bad account ID %s" % (account_id,))
            continue
        staked = value['Account']['account'].pop('staked')
        value['Account']['account']['locked'] = staked
    elif 'AccessKey' in value:
        account_id = value['AccessKey']['account_id']
        if not is_valid_account.match(account_id):
            print("Nuked access key for bad account ID %s" % (account_id,))
            continue
    elif 'Contract' in value:
        account_id = value['Contract']['account_id']
        if not is_valid_account.match(account_id):
            print("Nuked contract for bad account ID %s" % (account_id,))
            continue
    elif 'Data' in value:
        key = base64.b64decode(value['Data']['key']).decode('utf-8')
        account_id = key[1:key.find(',')]
        if not is_valid_account.match(account_id):
            print("Nuked data for bad account ID %s" % (account_id,))
            continue

    new_records.append(value)

q['records'] = new_records
q['protocol_version'] = 4
q['dynamic_resharding'] = False
q['block_producers_per_shard'] = [50] * 8
q['avg_fisherman_per_shard'] = [0] * 8
q['num_blocks_per_year'] = 31536000
q['gas_limit'] = 10000000
q['gas_price'] = 100
q['gas_price_adjustment_rate'] = 1
q['developer_reward_percentage'] = 30
q['protocol_reward_percentage'] = 10
q['max_inflation_rate'] = 5
q['total_supply'] = 0
q['protocol_treasury_account'] = 'near'
q['transaction_validity_period'] = 1000
q['validator_kickout_threshold'] = 90
ext_costs = json.loads('''{
          "input_base": 1,
          "input_per_byte": 1,
          "storage_read_base": 1,
          "storage_read_key_byte": 1,
          "storage_read_value_byte": 1,
          "storage_write_base": 1,
          "storage_write_key_byte": 1,
          "storage_write_value_byte": 1,
          "storage_has_key_base": 1,
          "storage_has_key_byte": 1,
          "storage_remove_base": 1,
          "storage_remove_key_byte": 1,
          "storage_remove_ret_value_byte": 1,
          "storage_iter_create_prefix_base": 1,
          "storage_iter_create_range_base": 1,
          "storage_iter_create_key_byte": 1,
          "storage_iter_next_base": 1,
          "storage_iter_next_key_byte": 1,
          "storage_iter_next_value_byte": 1,
          "read_register_base": 1,
          "read_register_byte": 1,
          "write_register_base": 1,
          "write_register_byte": 1,
          "read_memory_base": 1,
          "read_memory_byte": 1,
          "write_memory_base": 1,
          "write_memory_byte": 1,
          "account_balance": 1,
          "prepaid_gas": 1,
          "used_gas": 1,
          "random_seed_base": 1,
          "random_seed_per_byte": 1,
          "sha256": 1,
          "sha256_byte": 1,
          "attached_deposit": 1,
          "storage_usage": 1,
          "block_index": 1,
          "block_timestamp": 1,
          "current_account_id": 1,
          "current_account_id_byte": 1,
          "signer_account_id": 1,
          "signer_account_id_byte": 1,
          "signer_account_pk": 1,
          "signer_account_pk_byte": 1,
          "predecessor_account_id": 1,
          "predecessor_account_id_byte": 1,
          "promise_and_base": 1,
          "promise_and_per_promise": 1,
          "promise_result_base": 1,
          "promise_result_byte": 1,
          "promise_results_count": 1,
          "promise_return": 1,
          "log_base": 1,
          "log_byte": 1
        }''')
burnt_gas_reward = json.loads('''{
          "denominator": 10,
          "numerator": 3
        }''')
q['runtime_config']['transaction_costs']['ext_costs'] = ext_costs
q['runtime_config']['transaction_costs']['burnt_gas_reward'] = burnt_gas_reward
q['runtime_config']['wasm_config']['runtime_fees']['ext_costs'] = ext_costs
q['runtime_config']['wasm_config']['runtime_fees']['burnt_gas_reward'] = burnt_gas_reward


open(filename + '.v4', 'w').write(json.dumps(q, indent=2))
