import json
import os
import random

import delegator
import pytest
from retrying import retry

from near.pynear.lib import NearLib

nearlib = NearLib('http://localhost:3030/')


@retry(stop_max_attempt_number=5, wait_fixed=1000)
def check_devnet_health(process):
    if not process.is_alive:
        return False

    return nearlib.check_health()


@pytest.fixture
def make_devnet(request):
    def _make_devnet(base_dir):
        devnet_exe = os.environ['NEAR_DEVNET_EXE']
        command = "{devnet_exe} -d {base_dir} --test-block-period 5" \
            .format(devnet_exe=devnet_exe, base_dir=base_dir)
        process = delegator.run(command, block=False)
        request.addfinalizer(process.kill)
        return check_devnet_health(process)

    return _make_devnet


@pytest.fixture(scope='session')
def hello_wasm_path():
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    hello_dir = os.path.join(cur_dir, '../../../tests/hello')
    command = 'npm install && npm run build'
    process = delegator.run(command, cwd=hello_dir)
    assert process.return_code == 0, process.err
    return os.path.join(hello_dir, '../hello.wasm')


class Helpers(object):
    @staticmethod
    def run_command(command):
        command = "python -m near.pynear.cli {}".format(command)
        process = delegator.run(command)
        assert process.return_code == 0, process.err
        return process.out

    @classmethod
    def get_latest_beacon_block(cls):
        command = 'view_latest_beacon_block'
        out = cls.run_command(command)
        return json.loads(out)

    @classmethod
    def view_account(cls, account_name=None):
        command = 'view_account'
        if account_name is not None:
            command = "{} --account {}".format(command, account_name)

        out = cls.run_command(command)
        return json.loads(out)

    @classmethod
    def get_latest_shard_block(cls):
        command = 'view_latest_shard_block'
        out = cls.run_command(command)
        return json.loads(out)

    @classmethod
    def create_account(cls, account_id):
        command = "create_account {} 10".format(account_id)
        cls.run_command(command)

        @retry(stop_max_attempt_number=5, wait_fixed=1000)
        def _wait_for_account():
            return cls.view_account(account_id)

        return _wait_for_account()

    @classmethod
    def deploy_contract(cls, wasm_path):
        buster = random.randint(0, 10000)
        contract_name = "test_contract_{}".format(buster)
        cls.create_account(contract_name)

        command = "deploy {} {}".format(contract_name, wasm_path)
        cls.run_command(command)

        @retry(stop_max_attempt_number=5, wait_fixed=1000)
        def _wait_for_contract():
            return cls.view_account(contract_name)

        contract = _wait_for_contract()
        assert contract['account_id'] == contract_name
        return contract


def test_view_latest_beacon_block(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    Helpers.get_latest_beacon_block()


def test_get_beacon_block_by_hash(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    latest_block = Helpers.get_latest_beacon_block()
    hash_ = latest_block['hash']
    command = "get_beacon_block_by_hash {}".format(hash_)
    out = Helpers.run_command(command)
    assert latest_block == json.loads(out)


def test_view_latest_shard_block(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    Helpers.get_latest_shard_block()


def test_get_shard_block_by_hash(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    latest_block = Helpers.get_latest_shard_block()
    hash_ = latest_block['hash']
    command = "get_shard_block_by_hash {}".format(hash_)
    out = Helpers.run_command(command)
    assert latest_block == json.loads(out)


def test_view_account(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    Helpers.view_account()


def test_create_account(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    account_id = 'eve.near'
    Helpers.create_account(account_id)


def test_deploy_contract(make_devnet, tmpdir, hello_wasm_path):
    assert make_devnet(tmpdir)
    Helpers.deploy_contract(hello_wasm_path)


def test_send_money(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    receiver = 'send_money_test.near'
    Helpers.create_account(receiver)
    command = "send_money --receiver {} --amount 1".format(receiver)
    Helpers.run_command(command)

    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def _wait_for_balance_change():
        account = Helpers.view_account(receiver)
        assert account['amount'] == 11

    _wait_for_balance_change()


def test_set_get_values(make_devnet, tmpdir, hello_wasm_path):
    assert make_devnet(tmpdir)
    contract = Helpers.deploy_contract(hello_wasm_path)
    contract_name = contract['account_id']
    value = 'test'
    args = {'value': value}
    command = "schedule_function_call {} setValue --args '{}'" \
        .format(contract_name, json.dumps(args))
    Helpers.run_command(command)

    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def _wait_for_state_change():
        command_ = "call_view_function {} getValue --args {{}}" \
            .format(contract_name)
        out = Helpers.run_command(command_)
        data = json.loads(out)
        assert data['result'] == value

    _wait_for_state_change()


def test_view_state(make_devnet, tmpdir, hello_wasm_path):
    assert make_devnet(tmpdir)
    contract = Helpers.deploy_contract(hello_wasm_path)
    contract_name = contract['account_id']
    command = "view_state {}".format(contract_name)
    out = Helpers.run_command(command)
    data = json.loads(out)
    assert data['values'] == {}


def test_swap_key(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    public_key = nearlib.keystore.create_key_pair('alice.near')
    command = "swap_key {} {}".format(public_key, public_key)
    Helpers.run_command(command)
