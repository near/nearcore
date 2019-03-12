import os
import sys

import delegator
import pytest
import subprocess
from retrying import retry

from near.pynear.lib import NearLib


@retry(stop_max_attempt_number=5, wait_fixed=1000)
def check_devnet_health(process, nearlib):
    if not delegator.pid_exists(process.pid):
        return False

    return nearlib.check_health()


@pytest.fixture(scope='session')
def get_incrementing_number():
    # jank because py2 does not support nonlocal keyword
    # https://stackoverflow.com/a/3190783
    d = {'latest': -1}

    def _get_incrementing_number():
        d['latest'] += 1
        return d['latest']

    return _get_incrementing_number


@pytest.fixture
def make_devnet(request, get_incrementing_number):
    def _make_devnet(base_dir):
        port = 3030 + get_incrementing_number()
        devnet_exe = os.environ['NEAR_DEVNET_EXE']
        command = "{devnet_exe} -d {base_dir} --rpc_port {port} " \
                  "--test-block-period 5" \
            .format(devnet_exe=devnet_exe, base_dir=base_dir, port=port)
        process = subprocess.Popen(command.split(' '),  stdout=sys.stdout, stderr=sys.stdout)
        request.addfinalizer(process.kill)
        nearlib = NearLib("http://localhost:{}/".format(port))
        assert check_devnet_health(process, nearlib)
        return port

    return _make_devnet
