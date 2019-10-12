# Spins up 4 remote instances, start 3 validator node and 1 observer on it
import sys
import time
import subprocess
import os

sys.path.append('lib')

from cluster import init_cluster, start_cluster

args = (3, 1, 1, {
    'local': False,
    'near_root': '../target/debug/',
    'remote': {
        'instance_name': 'near-pytest',
        'commit': 'staging',
    }
}, [], [])

# init_cluster(*args)
# subprocess.run([os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../scripts/create_instance_pool.sh"),
#                 "near-pytest",
#                 "us-west2-a us-west2-b us-west2-c us-west2-a"])
# start_cluster(*args)

