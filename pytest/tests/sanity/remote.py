# Spins up 4 remote instances, start 3 validator node and 1 observer on it
import sys
import time
import subprocess
import os

sys.path.append('lib')

from cluster import init_cluster, start_cluster, GCloudNode
import retrying as retry

args = (3, 1, 1, {
    'local': False,
    'near_root': '../target/debug/',
    'remote': {
        'instance_name': 'near-pytest',
    }
}, [], [])

init_cluster(*args)
subprocess.run([os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../scripts/create_instance_pool.sh"),
                "near-pytest",
                "us-west2-a us-west2-b us-west2-c us-west2-a"])
node_dirs = subprocess.check_output("find ~/.near/* -maxdepth 0", shell=True).decode('utf-8').strip().split('\n')

g = GCloudNode('near-pytest-0', node_dirs[0])
assert g.machine_status() == 'RUNNING'
g.is_ready()
print(g.addr())

start_cluster(*args)

g.change_version('staging')
retry.retry(lambda: g.is_ready(), 1200)

g.update_config_files("/tmp/near/test0")
g.start()

g.turn_off_machine()
assert g.machine_status() == 'STOPPED'

g.turn_on_machine()
assert g.machine_status() == 'RUNNING'

g.start()

subprocess.run([os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../scripts/delete_instance_pool.sh")])