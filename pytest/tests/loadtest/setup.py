import mocknet_helpers
import account
import key

validator_key = key.Key.from_json_file(
    "/Users/michalski/.near_tmp/6/validator_key.json")

base_block_hash = mocknet_helpers.get_latest_block_hash()
nonce = mocknet_helpers.get_nonce_for_key(validator_key)

my_account = account.Account(validator_key,
                             init_nonce=nonce,
                             base_block_hash=base_block_hash,
                             rpc_infos=[("localhost", "3030")])

account_name = "shard12.test.near"
tx = my_account.send_create_account_tx(account_name)
print(tx)
account_key = key.Key(account_name, validator_key.pk, validator_key.sk)
base_block_hash = mocknet_helpers.get_latest_block_hash()

new_account = account.Account(account_key,
                              mocknet_helpers.get_nonce_for_key(account_key),
                              base_block_hash=base_block_hash,
                              rpc_infos=[("localhost", "3030")])

new_account.send_deploy_contract_tx(
    "pytest/tests/loadtest/contract/target/wasm32-unknown-unknown/release/loadtest_contract.wasm"
)
