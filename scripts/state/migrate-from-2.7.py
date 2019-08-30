#!/usr/bin/python

import json
import re
import os
import base64

filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../near/res/testnet.json')
q = json.loads(open(filename).read())
new_records = []
q['validators'][0]['account_id'] = 'near'

is_valid_account = re.compile(r"^(([a-z\d]+[\-_])*[a-z\d]+[\.@])*([a-z\d]+[\-_])*[a-z\d]+$")
assert(is_valid_account.match("near"))

nuked_accounts = set()

for value in q['records'][0]:
    if 'Account' in value:
        if value['Account']['account_id'] == '.near':
            value['Account']['account_id'] = 'near'

        account_id = value['Account']['account_id']

        if not is_valid_account.match(account_id):
            print("Bad account ID, nuking account %s" % (account_id,))
            nuked_accounts.add(account_id)
            continue

        amount = int(value['Account']['account']['amount'])
        staked = int(value['Account']['account']['staked'])

        if amount + staked < 1000000:
            print("Not a millionaire (%d), nuking account %s" % (amount + staked, account_id))
            nuked_accounts.add(account_id)
            continue

        value['Account']['account']['storage_paid_at'] = 0
        value['Account']['account']['code_hash'] = '11111111111111111111111111111111'

        nonce = value['Account']['account'].pop('nonce')

        public_keys = value['Account']['account'].pop('public_keys')
        new_records.append(value)
        for public_key in public_keys:
            new_records.append({
                'AccessKey': {
                    'access_key': {
                        'nonce': nonce,
                        'permission': 'FullAccess'
                    },
                    'public_key': 'ed25519:%s' % public_key,
                    'account_id': account_id
                }
            })
    elif 'AccessKey' in value:
        # Skipping all access keys, since the amount is "0"
        amount = value['AccessKey']['access_key']['amount']
        assert(amount == "0")
        continue
    elif 'Contract' in value:
        # Skipping all contracts, since the runtime API has changed
        continue
    elif 'Data' in value:
        # Parsing data key to check if we nuked given account id
        key = base64.b64decode(value['Data']['key'])
        account_id = key[:key.find(',')]
        if account_id in nuked_accounts:
            print("Nuked data for account %d " % (account_id,))
            continue
        new_records.append(value)
    else:
        new_records.append(value)

q['records'][0] = new_records
q['protocol_version'] = 3
q['runtime_config']['storage_cost_byte_per_block'] = "1"
q['runtime_config']['account_length_baseline_cost_per_block'] = "6561"
q['runtime_config']['poke_threshold'] = 60
open(filename + '.v3', 'w').write(json.dumps(q, indent=2))
