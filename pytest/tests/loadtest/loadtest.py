from time import sleep, time
from tqdm import tqdm
import mocknet_helpers
import account
import key
import base64

validator_key = key.Key.from_json_file(
    "/Users/michalski/.near_tmp/6/validator_key.json")

base_block_hash = mocknet_helpers.get_latest_block_hash()
nonce = mocknet_helpers.get_nonce_for_key(validator_key)

my_account = account.Account(validator_key,
                             init_nonce=nonce,
                             base_block_hash=base_block_hash,
                             rpc_infos=[("localhost", "3030")])

my_account.send_call_contract_raw_tx(
    contract_id="shard12.test.near",  #f"shard{y}.test.near",
    method_name="reset_before_do_even_more_work",
    args=f'{{"how_many": 400}}'.encode("utf-8"),
    deposit=0)

results = []
for i in tqdm(range(100)):
    for y in range(5):
        result = my_account.send_call_contract_raw_tx(
            contract_id="shard12.test.near",  #f"shard{y}.test.near",
            method_name="do_even_more_work",
            args=f'{{"how_many": {400 + i}}}'.encode("utf-8"),
            deposit=0)
        results.append(result)

print(results[-50:])

res = my_account.send_call_contract_raw_tx(
    contract_id="shard12.test.near",  #f"shard{y}.test.near",
    method_name="get_more_work",
    args=f'{{"how_many": 400}}'.encode("utf-8"),
    deposit=0)

print(res)

while True:
    tx_status = mocknet_helpers.json_rpc(
        "EXPERIMENTAL_tx_status", [res["result"], validator_key.account_id])

    print(tx_status)
    if 'error' in tx_status:
        sleep(3)
    else:
        outcome = base64.b64decode(
            tx_status['result']['status']['SuccessValue'])
        print(outcome)
        break
