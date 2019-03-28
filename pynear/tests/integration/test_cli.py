import json

from near.pynear.test_utils.cli import CliHelpers
from near.pynear.test_utils.fixtures import *


@pytest.fixture(scope='session')
def hello_wasm_path():
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    hello_dir = os.path.join(cur_dir, '../../../tests/hello')
    command = 'npm install && npm run build'
    process = delegator.run(command, cwd=hello_dir)
    assert process.return_code == 0, process.err
    return os.path.join(hello_dir, '../hello.wasm')


def test_view_latest_beacon_block(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    CliHelpers(port).get_latest_beacon_block()


def test_get_beacon_block_by_hash(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    latest_block = CliHelpers(port).get_latest_beacon_block()
    hash_ = latest_block['hash']
    command = "get_beacon_block_by_hash {}".format(hash_)
    out = CliHelpers(port).run_command(command)
    assert latest_block == json.loads(out)


def test_view_latest_shard_block(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    CliHelpers(port).get_latest_shard_block()


def test_get_shard_block_by_hash(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    latest_block = CliHelpers(port).get_latest_shard_block()
    hash_ = latest_block['hash']
    command = "get_shard_block_by_hash {}".format(hash_)
    out = CliHelpers(port).run_command(command)
    assert latest_block == json.loads(out)


def test_view_account(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    CliHelpers(port).view_account()


def test_create_account(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    account_id = 'eve.near'
    CliHelpers(port).create_account(account_id)


def test_deploy_contract(
        make_devnet,
        tmpdir,
        get_incrementing_number,
        hello_wasm_path,
):
    port = make_devnet(tmpdir)
    buster = get_incrementing_number()
    contract_name = "test_contract_{}".format(buster)
    CliHelpers(port).deploy_contract(contract_name, hello_wasm_path)


def test_send_money(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    receiver = 'send_money_test.near'
    CliHelpers(port).create_account(receiver)
    command = "send_money --receiver {} --amount 1".format(receiver)
    CliHelpers(port).run_command(command)

    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def _wait_for_balance_change():
        account = CliHelpers(port).view_account(receiver)
        assert account['amount'] == 11

    _wait_for_balance_change()


def test_set_get_values(
        make_devnet,
        tmpdir,
        get_incrementing_number,
        hello_wasm_path,
):
    port = make_devnet(tmpdir)
    buster = get_incrementing_number()
    contract_name = "test_contract_{}".format(buster)
    contract, _ = CliHelpers(port).deploy_contract(contract_name, hello_wasm_path)
    contract_name = contract['account_id']
    value = 'test'
    args = {'value': value}
    command = "schedule_function_call {} setValue --args '{}'" \
        .format(contract_name, json.dumps(args))
    CliHelpers(port).run_command(command)

    @retry(stop_max_attempt_number=5, wait_fixed=1000)
    def _wait_for_state_change():
        command_ = "call_view_function {} getValue --args {{}}" \
            .format(contract_name)
        out = CliHelpers(port).run_command(command_)
        data = json.loads(out)
        assert data == value

    _wait_for_state_change()


def test_view_state(
        make_devnet,
        tmpdir,
        get_incrementing_number,
        hello_wasm_path,
):
    port = make_devnet(tmpdir)
    buster = get_incrementing_number()
    contract_name = "test_contract_{}".format(buster)
    contract, _ = CliHelpers(port).deploy_contract(contract_name, hello_wasm_path)
    contract_name = contract['account_id']
    command = "view_state {}".format(contract_name)
    out = CliHelpers(port).run_command(command)
    data = json.loads(out)
    assert data['values'] == {}


def test_swap_key(make_devnet, tmpdir):
    port = make_devnet(tmpdir)
    public_key = NearLib().keystore.create_key_pair('alice.near')
    command = "swap_key {} {}".format(public_key, public_key)
    CliHelpers(port).run_command(command)
