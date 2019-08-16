import json
filename = os.path.dirname(os.path.realpath(__file__)) + '../../res/testnet.json'
q = json.loads(open(filename).read())
new_records = []
for value in q['records'][0]:
    if 'Account' in value:
        value['Account']['account']['storage_paid_at'] = 0
        value['Account']['account']['code_hash'] = '11111111111111111111111111111111'

        nonce = value['Account']['account'].pop('nonce')
        account_id = value['Account']['account_id']

        public_keys = value['Account']['account'].pop('public_key')
        new_records.push(value)
        for public_key in public_keys:
            new_records.append({
                'AccessKey': {
                    'access_key': {
                        'nonce': nonce,
                        'permission': {
                            'FullAccess': True,
                        }
                    },
                    'public_key': public_key,
                    'account_id': account_id
                }
            })
    elif 'AccessKey' in value:
        # Skipping all access keys, since the amount is "0"
        amount = value['AccessKey']['access_key']['amount']
        assert(amount == "0")
    elif 'Contract' in value:
        # Skipping all contracts, since the runtime API has changed
        pass
    else:
        new_records.push(value)
q['records'][0] = new_records
q['protocol_version'] = 3
open(filename + '.out', 'w').write(json.dumps(q, indent=2))
