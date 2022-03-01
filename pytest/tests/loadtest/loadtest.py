from time import sleep, time
from tqdm import tqdm
import mocknet_helpers
import account
import key
import base64
import argparse
from os.path import join

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Setup loadtest')

    parser.add_argument('--home', type=str, required=True)
    parser.add_argument('--num_accounts', type=int, default=5)
    parser.add_argument('--num_requests', type=int, default=50)
    args = parser.parse_args()

    validator_key = key.Key.from_json_file(join(args.home,
                                                "validator_key.json"))

    base_block_hash = mocknet_helpers.get_latest_block_hash()
    nonce = mocknet_helpers.get_nonce_for_key(validator_key)

    my_account = account.Account(validator_key,
                                 init_nonce=nonce,
                                 base_block_hash=base_block_hash,
                                 rpc_infos=[("localhost", "3030")])

    # First - 'reset' the counters in the contract.
    for y in range(args.num_accounts):
        my_account.send_call_contract_raw_tx(
            contract_id=f"shard{y}.test.near",
            method_name="reset_increment_many",
            args=f'{{"how_many": 400}}'.encode("utf-8"),
            deposit=0)

    results = []

    for i in tqdm(range(args.num_requests)):
        for y in range(args.num_accounts):
            result = my_account.send_call_contract_raw_tx(
                contract_id=f"shard{y}.test.near",
                method_name="increment_many",
                args=f'{{"how_many": {max(400 + i, 450)}}}'.encode("utf-8"),
                deposit=0)
            results.append(result)

    for y in range(args.num_accounts):
        res = my_account.send_call_contract_raw_tx(
            contract_id=f"shard{y}.test.near",
            method_name="get_increment_many",
            args='',
            deposit=0)
        print(f"Shard {y} asking for result: {res}")
        while True:
            tx_status = mocknet_helpers.json_rpc(
                "EXPERIMENTAL_tx_status",
                [res["result"], validator_key.account_id])
            #print(tx_status)
            if 'error' in tx_status:
                print("tx error: tx not ready yet")
                sleep(3)
            else:
                outcome = base64.b64decode(
                    tx_status['result']['status']['SuccessValue'])
                if int(outcome) == args.num_requests:
                    print(f"Shard {y}: PASS")
                else:
                    print(f"Shard {y} : FAIL {outcome} vs {args.num_accounts}")
                break
