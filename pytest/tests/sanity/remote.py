# Spins up 4 remote instances, start 3 validator node and 1 observer on it
import sys
import time

sys.path.append('lib')

from cluster import init_cluster, start_cluster

args = (3, 1, 1, {
    'local': False,
    'near_root': '../target/debug/',
    'remote': {
        'machine_type': 'n1-standard-4',
        'zones': ['us-west2-a'],
        'commit': 'staging',

    }
}, [], [])

start_cluster(*args)