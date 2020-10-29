import sys, time

sys.path.append('lib')

from cluster import start_cluster
from bridge import start_ganache, start_bridge

#ganache = start_ganache()
bridge = start_bridge()
nodes = start_cluster(2, 0, 4, None, [], {})

time.sleep(3)
status = nodes[0].get_status()
print(status)
status = nodes[1].get_status()
print(status)

bridge.init_near_contracts()

while True:
    pass
