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
import cluster as clusterlib
from account import JsonProvider, Account
from utils import load_binary_file, wait_for_blocks_or_timeout


class Cluster(object):

    def __init__(self, num_shards, config, genesis_config_changes, client_config_changes):
        if not config:
            config = clusterlib.load_config()
        if "node_root" not in config:
            config["node_root"] = os.path.expanduser("~/.near/")

        self.config = config
        self.num_shards = num_shards
        self.genesis_config_changes = genesis_config_changes
        self.client_config_changes = client_config_changes
        self.nodes = []

    def start(self, num_nodes, num_observers):
        assert len(self.nodes) == 0
        # TODO: this really should implement this better by taking apart the start_cluster funciton.
        self.nodes = clusterlib.start_cluster(num_nodes, num_observers, self.num_shards, self.config, self.genesis_config_changes, self.client_config_changes)

    def add_node(self, account_id):
        assert len(self.nodes) > 0
        node_id = len(self.nodes)
        base_node_root = os.path.join(self.config["node_root"], "test0")
        node_root = os.path.join(self.config["node_root"], "test%s" % node_id)
        os.mkdir(node_root)
        for filename in ["config.json", "genesis.json"]:
            shutil.copy(os.path.join(base_node_root, filename), os.path.join(node_root, filename))
        subprocess.check_output([os.path.join(self.config["near_root"], "keypair-generator"), "--home=%s" % node_root, "node-key"])
        subprocess.check_output([os.path.join(self.config["near_root"], "keypair-generator"), "--home=%s" % node_root, "--account-id=%s" % account_id, "validator-key"])
        node = clusterlib.spin_up_node(self.config, self.config["near_root"], node_root, node_id, self.nodes[0].node_key.pk, self.nodes[0].addr())
        self.nodes.append(node)
        return node_id

    def get_account_for_node(self, node_id):
        return Account(JsonProvider(self.nodes[node_id].rpc_addr()), self.nodes[node_id].signer_key, self.nodes[node_id].signer_key.account_id)


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


def nty(value):
    """Converts NAER to yoctoNEAR"""
    return int(value * (10 ** 24))


if __name__ == "__main__":
    cluster = Cluster(1, None, [["num_block_producer_seats", 10], ["num_block_producer_seats_per_shard", [10]], ["epoch_length", 10], ["block_producer_kickout_threshold", 40]], {})
    cluster.start(1, 0)

    # Spin up new node without any account yet.
    account_name = 'staker'
    stake_amount1 = 50000000
    stake_amount2 = 70000000
    node_id = cluster.add_node(account_name)

    # Deploy & init staking contract.
    master_account = cluster.get_account_for_node(0)
    contract_path = download_from_url('https://github.com/near/staking-contract/raw/master/res/staking_contract.wasm')
    stake_public_key = cluster.nodes[node_id].signer_key.pk.split(':')[1]
    master_account.create_deploy_and_init_contract(
        account_name, None, load_binary_file(contract_path), nty(100),
        {"owner": cluster.nodes[0].signer_key.account_id, "stake_public_key": stake_public_key})

    print(master_account.provider.get_account(account_name))

    # Create couple accounts to delegate.
    master_account.create_account('user1', master_account.signer.decoded_pk(), nty(stake_amount1 + 1))
    master_account.create_account('user2', master_account.signer.decoded_pk(), nty(stake_amount2 + 1))

    user1 = Account(master_account.provider, master_account.signer, 'user1')
    user1.function_call(account_name, 'deposit', {}, amount=nty(stake_amount1))
    user1.function_call(account_name, 'stake', {"amount": str(nty(stake_amount1))})

    def ping():
        master_account.function_call(account_name, 'ping', {})
        time.sleep(1)

    wait_for_blocks_or_timeout(cluster.nodes[node_id], 20, 120, ping)
    assert is_active_validator("staker")

    user2 = Account(master_account.provider, master_account.signer, 'user2')
    user2.function_call(account_name, 'deposit', {}, amount=nty(stake_amount2))
    user2.function_call(account_name, 'stake', {"amount": str(nty(stake_amount2))})

    user1.function_call(account_name, 'unstake', {"amount": str(nty(stake_amount1))})


    wait_for_blocks_or_timeout(cluster.nodes[node_id], 20, 120, ping)
    assert is_active_validator("staker")

    # user1.function_call(account_name, 'withdraw', {}, amount=nty(stake_amount))
    assert user1.view_function(account_name, 'get_user_balance', {}) == str(nty(stake_amount1))

    # account_state = user1.provider.get_account('user1')
    # assert account_state["amount"] > nty(50000000)
