from rc import gcloud, pmap, run
from distutils.util import strtobool
import sys
import datetime

sys.path.append('lib')
from utils import user_name

machines = gcloud.list()
node_prefix = sys.argv[1] if len(sys.argv) >= 2 else f"pytest-node-{user_name()}"
nodes = list(filter(lambda m: m.name.startswith(node_prefix), machines))

log_file = sys.argv[2] if len(sys.argv) >= 3 else "produce_record.txt"

collected_place = f'/tmp/near/collected_logs_{datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")}'

run(['mkdir', '-p', collected_place])

def collect_file(node):
    print(f'Download file from {node.name}')
    node.download('/tmp/python-rc.log', f'{collected_place}/{node.name}.txt')
    print(f'Download file from {node.name} finished')


pmap(collect_file, nodes)
print(f'All download finish, log collected at {collected_place}')
