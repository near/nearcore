import json
import os

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


def test_view_latest_beacon_block(make_devnet, tmpdir):
    assert make_devnet(tmpdir)
    command = "pynear view_latest_beacon_block"
    process = delegator.run(command)
    assert process.return_code == 0
    json.loads(process.out)
