import subprocess
import mocknet_helpers
import account
import key
import argparse
from os.path import join

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Setup loadtest')

    parser.add_argument('--home', type=str, required=True)
    parser.add_argument('--num_accounts', type=int, default=5)
    parser.add_argument('--host', type=str, default='127.0.0.1')
    parser.add_argument('--account_id', type=str, default=None)
    parser.add_argument('--contract_dir',
                        type=str,
                        default='pytest/tests/loadtest/contract')
    args = parser.parse_args()

    print("Compiling contract")
    subprocess.check_call(args=[
        "cargo", "build", "--target", "wasm32-unknown-unknown", "--release"
    ],
                          cwd=args.contract_dir)

    for i in range(args.num_accounts):
        account_name = args.account_id or f"shard{i}"

        shard_key = key.Key.from_json_file(
            join(args.home, f"{account_name}_key.json"))

        base_block_hash = mocknet_helpers.get_latest_block_hash(addr=args.host)
        nonce = mocknet_helpers.get_nonce_for_key(shard_key, addr=args.host)

        shard_account = account.Account(shard_key,
                                        init_nonce=nonce,
                                        base_block_hash=base_block_hash,
                                        rpc_infos=[(args.host, "3030")])

        shard_account.send_deploy_contract_tx(
            join(
                args.contract_dir,
                "target/wasm32-unknown-unknown/release/loadtest_contract.wasm"))
