#!/usr/bin/env python

# Migrate from 0.3.x state to 0.4 (sharded).

import json
import re
import os
import base64

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())
new_records = []

# TODO: uncomment when we migrate from 0.3 to 0.4
#q['records'] = q['records'][0]

for value in q['records']:
    if 'Account' in value:
        staked = value['Account']['account'].pop('staked')
        value['Account']['account']['locked'] = staked
        value['Account']['account']['desired_stake'] = staked
    new_records.append(value)

q['records'] = new_records
q['protocol_version'] = 4
q['dynamic_resharding'] = False
q['block_producers_per_shard'] = [50, 50, 50, 50]
q['avg_fisherman_per_shard'] = [0, 0, 0, 0]
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

open(filename + '.v4', 'w').write(json.dumps(q, indent=2))
