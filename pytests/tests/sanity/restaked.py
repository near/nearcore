# Spins up four validating nodes, and kills one of them.
# Starts a restaking service that keeps this node still as an active validator as it gets kicked out.
# Ensures that this node is current validator after 5 epochs.

import sys
import time
import os
import subprocess
import signal
import atexit

sys.path.append('lib')

from cluster import start_cluster, load_config
from configured_logger import logger

TIMEOUT = 150
BLOCKS = 50
EPOCH_LENGTH = 10


def atexit_stop_restaked(pid):
    logger.info("Cleaning up restaked on script exit")
    os.kill(pid, signal.SIGKILL)


def start_restaked(node_dir, rpc_port, config):
    if not config:
        config = load_config()
    near_root = config['near_root']
    command = [
        near_root + 'restaked',
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

started = time.time()

restaked_pid = start_restaked(nodes[0].node_dir, nodes[1].rpc_port, {})
nodes[0].kill()

while time.time() - started < TIMEOUT:
    status = nodes[1].get_status()
    height = status['sync_info']['latest_block_height']
    # epoch boundary may have shifted due to validator being offline
    if height > EPOCH_LENGTH * 5 + 5:
        # 5 epochs later.
        validators = nodes[1].get_validators()['result']
        present = False
        for validator in validators['current_validators']:
            if validator['account_id'] == 'test0':
                present = True
                break
        if not present:
            assert False, "Validator test0 is not in current validators after 5 epochs"
        else:
            break
    time.sleep(1)
