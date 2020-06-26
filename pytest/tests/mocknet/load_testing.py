# Force each node to submit many transactions for
# about 20 minutes. Monitor the block production time
# stays consistent.

import sys, time
from rc import pmap

sys.path.append('lib')
import mocknet

nodes = mocknet.get_nodes()

mocknet.setup_python_environments(nodes)
mocknet.start_load_test_helpers(nodes)
