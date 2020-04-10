# Spins up one node, deploy staking contract, spin up another node that stakes with this staking contract,
# Delegate from other accounts, observe that validation happens by both nodes.
# Undelegate, correct rewards are returned and the new validator is removed.

import os, sys, time
import tempfile
import subprocess

sys.path.append('lib')
from cluster import start_cluster
from account import JsonProvider, Account
from utils import load_binary_file, compile_rust_contract


def download_from_url(url):
    output_filename = os.path.join("/tmp/", next(tempfile._get_candidate_names()))
    subprocess.check_output(['curl', '--proto', '=https', '--tlsv1.2',
                             '-sSfL', url, '-o', output_filename])
    return output_filename


if __name__ == "__main__":
    nodes = start_cluster(1, 0, 1, None, [["epoch_length", 10], ["block_producer_kickout_threshold", 80]], {})

    master_account = Account(JsonProvider(nodes[0].rpc_addr()), nodes[0].signer_key, nodes[0].signer_key.account_id)

    contract_path = download_from_url('https://github.com/near/staking-contract/raw/master/res/staking_contract.wasm')
    master_account.create_and_deploy_contract('staker', None, load_binary_file(contract_path), 1)
