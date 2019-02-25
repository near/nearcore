import json
import random

import delegator
from retrying import retry


def run_command(command):
    command = "python -m near.pynear.cli {}".format(command)
    process = delegator.run(command)
    assert process.return_code == 0, process.err
    return process.out


def get_latest_beacon_block():
    command = 'view_latest_beacon_block'
    out = run_command(command)
    return json.loads(out)


def view_account(account_name=None):
    command = 'view_account'
    if account_name is not None:
        command = "{} --account {}".format(command, account_name)

    out = run_command(command)
    return json.loads(out)


def get_latest_shard_block():
    command = 'view_latest_shard_block'
    out = run_command(command)
    return json.loads(out)


def create_account(account_id):
    command = "create_account {} 10".format(account_id)
    run_command(command)

    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def _wait_for_account():
        return view_account(account_id)

    return _wait_for_account()


def deploy_contract(wasm_path):
    buster = random.randint(0, 10000)
    contract_name = "test_contract_{}".format(buster)
    create_account(contract_name)

    command = "deploy {} {}".format(contract_name, wasm_path)
    run_command(command)

    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def _wait_for_contract():
        return view_account(contract_name)

    contract = _wait_for_contract()
    assert contract['account_id'] == contract_name
    return contract
