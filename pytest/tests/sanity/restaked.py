#!/usr/bin/env python3
# Spins up four validating nodes, and kills one of them.
# Starts a restaking service that keeps this node still as an active validator as it gets kicked out.
# Ensures that this node is current validator after 5 epochs.

import sys
import time
import os
import subprocess
import signal
import atexit
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, load_config
from configured_logger import logger
import utils

TIMEOUT = 150
EPOCH_LENGTH = 10


def atexit_stop_restaked(pid):
    logger.info("Cleaning up restaked on script exit")
    os.kill(pid, signal.SIGKILL)


def start_restaked(node_dir, rpc_port, config):
    if not config:
        config = load_config()
    near_root = config['near_root']
    command = [
        os.path.join(near_root, 'restaked'),
        '--rpc-url=127.0.0.1:%d' % rpc_port, '--wait-period=1'
    ]
    if node_dir:
        command.extend(['--home', node_dir])
    pid = subprocess.Popen(command).pid
    logger.info("Starting restaked for %s, rpc = 0.0.0.0:%d" %
                (node_dir, rpc_port))
    atexit.register(atexit_stop_restaked, pid)


# Local:
nodes = start_cluster(
    4, 0, 1, None,
    [["epoch_length", EPOCH_LENGTH], ["block_producer_kickout_threshold", 80]],
    {})

# Remote:
# NEAR_PYTEST_CONFIG=remote.json python tests/sanity/block_production.py

# Wait until at least one block is generated since otherwise we will get
# UnknownEpoch errors inside of restaked binary when asking for validators info.
utils.wait_for_blocks(nodes[1], target=1, timeout=TIMEOUT, poll_interval=0.1)

restaked_pid = start_restaked(nodes[0].node_dir, nodes[1].rpc_port, {})
nodes[0].kill()

# Epoch boundary may have shifted due to validator being offline
utils.wait_for_blocks(nodes[1], target=EPOCH_LENGTH * 5 + 5, timeout=TIMEOUT)

# 5 epochs later.
validators = nodes[1].get_validators()['result']
present = any(validator['account_id'] == 'test0'
              for validator in validators['current_validators'])
assert present, 'Validator test0 is not in current validators after 5 epochs'
