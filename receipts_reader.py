import json
import re

with open("./receipts.log", "r") as f:
    source = f.read().splitlines()
    receipt_ids = set(re.search(r'receipt_id: \"(.*?)\"', s).group(1) for s in source)
    print(receipt_ids)

with open("./mainnet_restored_receipts.json", "r") as f:
    data = json.loads(f.read())
    receipts = data['0']
    receipt_ids_my = set(x['receipt_id'] for x in receipts)
    print(receipt_ids_my)

print(receipt_ids - receipt_ids_my)
print(receipt_ids_my - receipt_ids)
# def dict_from_str(dict_str):
#     while True:
#
#         try:
#             dict_ = eval(dict_str)
#         except NameError as e:
#             key = e.message.split("'")[1]
#             dict_str = dict_str.replace(key, "'{}'".format(key))
#         else:
#             return dict_
#
