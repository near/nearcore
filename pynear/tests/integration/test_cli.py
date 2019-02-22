import json
import os
import sys

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


class Helpers(object):
    @staticmethod
    def get_latest_beacon_block():
        command = 'pynear view_latest_beacon_block'
        process = delegator.run(command)
        assert process.return_code == 0, process.err
        return json.loads(process.out)

    @staticmethod
    def view_account(account_name=None):
        command = 'pynear view_account'
        if account_name is not None:
            command = "{} {}".format(command, account_name)

        process = delegator.run(command)
        assert process.return_code == 0, process.err
        return json.loads(process.out)

    @staticmethod
    def get_latest_shard_block():
        command = 'pynear view_latest_shard_block'
        process = delegator.run(command)
        assert process.return_code == 0, process.err
        return json.loads(process.out)


def test_view_latest_beacon_block(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    Helpers.get_latest_beacon_block()


def test_get_beacon_block_by_hash(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    latest_block = Helpers.get_latest_beacon_block()
    hash_ = latest_block['hash']
    command = "pynear get_beacon_block_by_hash {}".format(hash_)
    process = delegator.run(command)
    assert latest_block == json.loads(process.out)


def test_view_latest_shard_block(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    Helpers.get_latest_shard_block()


def test_get_shard_block_by_hash(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    latest_block = Helpers.get_latest_shard_block()
    hash_ = latest_block['hash']
    command = "pynear get_shard_block_by_hash {}".format(hash_)
    process = delegator.run(command)
    assert latest_block == json.loads(process.out)


def test_view_account(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    Helpers.view_account()
