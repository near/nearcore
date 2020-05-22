"""
This migration implements StateRecord spec change by updating StateRecord::Data:
https://github.com/nearprotocol/NEPs/pull/39

Changes:
- Removes `storage_paid_at` from Account
- Modifies `StateRecord::Data` records
- fixing validator incorrect because of old state viewer

"""

import sys
import os
import json
from collections import OrderedDict
import base64

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 8

config['protocol_version'] = 9

for record in config['records']:
    if "Account" in record:
        record["Account"]["account"].pop("storage_paid_at", None)
    if "Data" in record:
        # Removing old joined key
        key = base64.b64decode(record["Data"].pop("key"))
        # Splitting key
        separator_pos = key.find(b',')
        assert (separator_pos > 0)
        account_id = key[1:separator_pos]
        data_key = key[separator_pos + 1:]
        record["Data"]["account_id"] = account_id.decode('utf-8')
        record["Data"]["data_key"] = base64.b64encode(data_key).decode('utf-8')

validators = {}
for v in config['validators']:
    validators[v['account_id']] = v

for record in config['records']:
    if "Account" in record:
        if record["Account"]["account"]["locked"] != "0" and record["Account"][
                "account_id"] not in validators:
            a = int(record["Account"]["account"]["amount"])
            l = int(record["Account"]["account"]["locked"])
            record["Account"]["account"]["locked"] = str(0)
            record["Account"]["account"]["amount"] = str(a + l)
        elif record["Account"]["account_id"] in validators:
            validators[record["Account"]["account_id"]]["amount"] = record[
                "Account"]["account"]["locked"]
json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
