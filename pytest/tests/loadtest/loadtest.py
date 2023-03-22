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
    parser.add_argument(
        '--sender_key_file',
        type=str,
        default="validator_key.json",
        help=
        "File in home directory that contains the key for the account that should be used to send all the requests."
    )
    parser.add_argument(
        '--num_accounts',
        type=int,
        default=5,
        help=
        "Accounts that contain the contract to run (either set the --num_accounts or comma separated list in --account_ids)"
    )
    parser.add_argument('--account_ids', type=str, default=None)

    parser.add_argument('--num_requests', type=int, default=50)
    parser.add_argument('--host', type=str, default='127.0.0.1')
    # Contract types:
    #  - storage - tries to do maximum amount of reads & writes (increments a large vector)
    #  - compute - tries to do maximum amount of compute (infinite loop)
    parser.add_argument('--contract_type',
                        type=str,
                        default='storage',
                        help="""# Contract types:
    #  - storage - tries to do maximum amount of reads & writes (increments a large vector)
    #  - compute - tries to do maximum amount of compute (infinite loop)
    #  - write - tries to write a lot of data to the contract state.""")
    args = parser.parse_args()

    accounts = [args.account_ids.split(",")] if args.account_ids else [
        f"shard{i}" for i in range(args.num_accounts)
    ]

    validator_key = key.Key.from_json_file(join(args.home,
                                                args.sender_key_file))

    base_block_hash = mocknet_helpers.get_latest_block_hash(addr=args.host)
    nonce = mocknet_helpers.get_nonce_for_key(validator_key, addr=args.host)

    my_account = account.Account(validator_key,
                                 init_nonce=nonce,
                                 base_block_hash=base_block_hash,
                                 rpc_infos=[(args.host, "3030")])

    # First - 'reset' the counters in the contract.
    for y in accounts:
        my_account.send_call_contract_raw_tx(
            contract_id=y,
            method_name="reset_increment_many",
            args=f'{{"how_many": 400}}'.encode("utf-8"),
            deposit=0)

    results = []

    contract_type = args.contract_type
    assert (contract_type in ['storage', 'compute', 'write'])

    if contract_type == "storage":
        method_name = "increment_many"
    if contract_type == "write":
        method_name = "write_many"
    if contract_type == "compute":
        method_name = "infinite_loop"

    for i in tqdm(range(args.num_requests)):
        for y in accounts:
            result = my_account.send_call_contract_raw_tx(
                contract_id=y,
                method_name=method_name,
                args=f'{{"how_many": {min(400 + i, 400)}}}'.encode("utf-8"),
                deposit=0)
            results.append(result)

    if contract_type == 'storage':
        # For 'storage' contracts - we can also check that all were executed successfully.
        for y in accounts:
            res = my_account.send_call_contract_raw_tx(
                contract_id=y,
                method_name="get_increment_many",
                args='',
                deposit=0)
            print(f"Shard {y} asking for result: {res}")
            result = mocknet_helpers.tx_result(res["result"],
                                               validator_key.account_id,
                                               addr=args.host,
                                               wait_for_success=True)
            outcome = base64.b64decode(result['status']['SuccessValue'])
            if int(outcome) == args.num_requests:
                print(f"Shard {y}: PASS")
            else:
                print(f"Shard {y} : FAIL {outcome} vs {args.num_requests}")
