import json

import delegator
from retrying import retry


class CliHelpers(object):
    def __init__(self, port=3030):
        self._url = "http://localhost:{}/".format(port)

    def run_command(self, command):
        command = "python -m near.pynear.cli {} -u {}".format(command, self._url)
        process = delegator.run(command)
        assert process.return_code == 0, process.err
        return process.out

    def get_latest_beacon_block(self):
        command = 'view_latest_beacon_block'
        out = self.run_command(command)
        return json.loads(out)

    def view_account(self, account_name=None):
        command = 'view_account'
        if account_name is not None:
            command = "{} --account {}".format(command, account_name)

        out = self.run_command(command)
        return json.loads(out)

    def get_latest_shard_block(self):
        command = 'view_latest_shard_block'
        out = self.run_command(command)
        return json.loads(out)

    def create_account(self, account_id):
        command = "create_account {} 10".format(account_id)
        self.run_command(command)

        @retry(stop_max_attempt_number=5, wait_fixed=1000)
        def _wait_for_account():
            return self.view_account(account_id)

        return _wait_for_account()

    def deploy_contract(self, contract_name, wasm_path=None):
        self.create_account(contract_name)

        command = "deploy {} {}".format(contract_name, wasm_path)
        out = self.run_command(command)
        data = json.loads(out)
        transaction_hash = data['hash']

        @retry(stop_max_attempt_number=5, wait_fixed=1000)
        def _wait_for_contract():
            return self.view_account(contract_name)

        contract = _wait_for_contract()
        assert contract['account_id'] == contract_name
        return contract, transaction_hash
