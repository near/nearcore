# Spins up one node, deploy staking contract, spin up another node that stakes with this staking contract.
# Initial validator test0 is staking 5M by default.
# Create two accounts user1 & user2 each with 5M.
# Delegate from user1, observe that validation happens by both nodes.
# Delegate from user2, observe that this node has more seats now.
# Undelegate from user1, correct rewards are returned and the new validator is removed.

import os, sys, time
import tempfile
import subprocess
import shutil

sys.path.append('lib')
from cluster import Cluster
from account import JsonProvider, Account
from utils import load_binary_file, wait_for_blocks_or_timeout, ntoy


def download_from_url(url):
    output_filename = os.path.join("/tmp/", next(tempfile._get_candidate_names()))
    subprocess.check_output(['curl', '--proto', '=https', '--tlsv1.2',
                             '-sSfL', url, '-o', output_filename])
    return output_filename


def is_active_validator(account_id):
    validators = master_account.provider.get_validators()
    print(validators)
    for validator in validators["current_validators"]:
        if validator["account_id"] == account_id:
            return True
    return False


if __name__ == "__main__":
    contract_path = sys.argv[1] if len(sys.argv) > 1 else None
    if not contract_path:
        contract_path = download_from_url('https://github.com/near/initial-contracts/raw/5c261dcf7cc250417b51cf85917d82492f0347a0/staking-pool-shares/res/staking_pool_with_shares.wasm')

    cluster = Cluster(1, None, [["num_block_producer_seats", 10], ["num_block_producer_seats_per_shard", [10]], ["epoch_length", 10], ["block_producer_kickout_threshold", 40]], {})
    cluster.start(1, 0)

    # Spin up new node without any account yet.
    account_name = 'staker'
    stake_amount1 = 50000000
    stake_amount2 = 70000000
    node_id = cluster.add_node(account_name)

    # Deploy & init staking contract.
    master_account = cluster.get_account_for_node(0)

    stake_public_key = cluster.nodes[node_id].signer_key.pk.split(':')[1]
    master_account.create_deploy_and_init_contract(
        account_name, None, load_binary_file(contract_path), ntoy(100),
        {"owner_id": cluster.nodes[0].signer_key.account_id, "stake_public_key": stake_public_key})

    print(master_account.provider.get_account(account_name))

    # Create couple accounts to delegate.
    master_account.create_account('user1', master_account.signer.decoded_pk(), ntoy(stake_amount1 + 1))
    master_account.create_account('user2', master_account.signer.decoded_pk(), ntoy(stake_amount2 + 1))

    user1 = Account(master_account.provider, master_account.signer, 'user1')
    print(">>> @user1 deposit call")
    user1.function_call(account_name, 'deposit', {}, amount=ntoy(stake_amount1))

    user1_unstaked = user1.view_function(account_name, 'get_account_unstaked_balance', {"account_id": "user1"})["result"]
    print(">>> @user1 unstaked balance =", user1_unstaked)

    print(">>> @user1 stake call")
    user1.function_call(account_name, 'stake', {"amount": str(ntoy(stake_amount1))})
    user1_stake = user1.view_function(account_name, 'get_account_staked_balance', {"account_id": "user1"})["result"]
    print(">>> @user1 stake =", user1_stake)

    def ping():
        master_account.function_call(account_name, 'ping', {})
        time.sleep(1)

    print(">>> Waiting for 20 epochs")

    wait_for_blocks_or_timeout(cluster.nodes[node_id], 20, 120, ping)
    assert is_active_validator("staker")

    user2 = Account(master_account.provider, master_account.signer, 'user2')
    print(">>> @user2 deposit call")
    user2.function_call(account_name, 'deposit', {}, amount=ntoy(stake_amount2))
    print(">>> @user2 stake call")
    user2.function_call(account_name, 'stake', {"amount": str(ntoy(stake_amount2))})

    # Unstake everything by user1 including rewards.
    user1_stake = user1.view_function(account_name, 'get_account_staked_balance', {"account_id": "user1"})["result"]
    print(">>> @user1 stake =", user1_stake)
    print(">>> @user1 unstake call")
    user1.function_call(account_name, 'unstake', {"amount": user1_stake})

    wait_for_blocks_or_timeout(cluster.nodes[node_id], 20, 120, ping)
    assert is_active_validator("staker")

    print(">>> Waiting for another 20 epochs")

    user1_left_stake = user1.view_function(account_name, 'get_account_staked_balance', {"account_id": "user1"})["result"]
    print(">>> @user1 stake =", user1_left_stake)
    assert user1_left_stake == "0", "%s != 0" % user1_left_stake
    user1_balance = user1.view_function(account_name, 'get_account_unstaked_balance', {"account_id": "user1"})["result"]
    print(">>> @user1 unstaked balance =", user1_balance)
    assert user1_balance == user1_stake, "%s != %s" % (user1_balance,  user1_stake)
    print(">>> @user1 withdraw call")
    user1.function_call(account_name, 'withdraw', {"amount": user1_balance})

    account_amount = int(user1.provider.get_account('user1')["amount"])
    assert account_amount > ntoy(stake_amount1)
    assert account_amount > int(user1_balance)

    user1_balance = user1.view_function(account_name, 'get_account_unstaked_balance', {"account_id": "user1"})["result"]
    print(">>> @user1 unstaked balance =", user1_balance)
    assert user1_balance == "0", "%s != 0" % user1_balance
